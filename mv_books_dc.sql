-- Dublin Core Materialized View with JSONB
-- Optimized for FullTextSearch.py query patterns
-- Fully aligned with libgutenberg DublinCoreMapping.py

-- Start transaction - if anything fails, everything rolls back
BEGIN;

-- Speed up MV creation with more RAM
SET LOCAL work_mem = '2GB';
SET LOCAL maintenance_work_mem = '4GB';
SET LOCAL max_parallel_workers_per_gather = 4;

-- Set to abort on any error
SET LOCAL client_min_messages = WARNING;

-- Create tsvector aggregate function if not exists
DO $$
BEGIN
    CREATE AGGREGATE tsvector_agg(tsvector) (
        SFUNC = tsvector_concat,
        STYPE = tsvector,
        INITCOND = ''
    );
EXCEPTION WHEN duplicate_function THEN
    NULL;
END $$;

DROP MATERIALIZED VIEW IF EXISTS mv_books_dc CASCADE;

CREATE MATERIALIZED VIEW mv_books_dc AS
SELECT 
    b.pk AS book_id,
    b.title,
    b.tsvec,                    -- Book tsvec (title + subjects + authors)
    -- Author tsvec (aggregated from all authors)
    COALESCE((
        SELECT tsvector_agg(au.tsvec)
        FROM mn_books_authors mba
        JOIN authors au ON mba.fk_authors = au.pk
        WHERE mba.fk_books = b.pk AND au.tsvec IS NOT NULL
    ), ''::tsvector) AS author_tsvec,
    -- Subject tsvec (aggregated from all subjects)
    COALESCE((
        SELECT tsvector_agg(s.tsvec)
        FROM mn_books_subjects mbs
        JOIN subjects s ON mbs.fk_subjects = s.pk
        WHERE mbs.fk_books = b.pk AND s.tsvec IS NOT NULL
    ), ''::tsvector) AS subject_tsvec,
    -- Bookshelf tsvec (aggregated from all bookshelves)
    COALESCE((
        SELECT tsvector_agg(bs.tsvec)
        FROM mn_books_bookshelves mbbs
        JOIN bookshelves bs ON mbbs.fk_bookshelves = bs.pk
        WHERE mbbs.fk_books = b.pk AND bs.tsvec IS NOT NULL
    ), ''::tsvector) AS bookshelf_tsvec,
    b.downloads,
    b.release_date,
    b.copyrighted,
    -- Has files check (common filter)
    EXISTS (
        SELECT 1 FROM files f 
        WHERE f.fk_books = b.pk 
        AND f.obsoleted = 0 
        AND f.diskstatus = 0
    ) AS has_files,
    -- Primary author name for fast filtering/display
    (
        SELECT au.author
        FROM mn_books_authors mba
        JOIN authors au ON mba.fk_authors = au.pk
        WHERE mba.fk_books = b.pk AND mba.heading = 1
        LIMIT 1
    ) AS primary_author,
    -- Primary author birth year for range filters
    (
        SELECT au.born_floor
        FROM mn_books_authors mba
        JOIN authors au ON mba.fk_authors = au.pk
        WHERE mba.fk_books = b.pk AND mba.heading = 1
        LIMIT 1
    ) AS author_birthdate,
    -- Primary author death year for range filters
    (
        SELECT au.died_floor
        FROM mn_books_authors mba
        JOIN authors au ON mba.fk_authors = au.pk
        WHERE mba.fk_books = b.pk AND mba.heading = 1
        LIMIT 1
    ) AS author_deathdate,
    -- Is audiobook flag (category PK 1, 2, 3, or 6 are Sound per dcmitypes table)
    EXISTS (
        SELECT 1 FROM mn_books_categories mbc
        WHERE mbc.fk_books = b.pk AND mbc.fk_categories IN (1, 2, 3, 6)
    ) AS is_audiobook,
    -- Primary language code for fast filtering
    (
        SELECT l.pk
        FROM mn_books_langs mbl
        JOIN langs l ON mbl.fk_langs = l.pk
        WHERE mbl.fk_books = b.pk
        LIMIT 1
    ) AS primary_language,
    -- Primary DCMI type for filtering
    (
        SELECT mbc.fk_categories
        FROM mn_books_categories mbc
        WHERE mbc.fk_books = b.pk
        LIMIT 1
    ) AS primary_dcmitype,
    -- Primary LoCC for fast filtering
    (
        SELECT lc.pk
        FROM mn_books_loccs mblc
        JOIN loccs lc ON mblc.fk_loccs = lc.pk
        WHERE mblc.fk_books = b.pk
        LIMIT 1
    ) AS primary_locc,
    -- Primary subject for display
    (
        SELECT s.subject
        FROM mn_books_subjects mbs
        JOIN subjects s ON mbs.fk_subjects = s.pk
        WHERE mbs.fk_books = b.pk
        LIMIT 1
    ) AS primary_subject,
    -- Primary bookshelf for display
    (
        SELECT bs.bookshelf
        FROM mn_books_bookshelves mbbs
        JOIN bookshelves bs ON mbbs.fk_bookshelves = bs.pk
        WHERE mbbs.fk_books = b.pk
        LIMIT 1
    ) AS primary_bookshelf,
    -- Author count (useful for "single author" vs "anthology" filtering)
    (
        SELECT COUNT(*)
        FROM mn_books_authors mba
        WHERE mba.fk_books = b.pk
    )::int AS author_count,
    -- File count (useful for "has multiple formats" filtering)
    (
        SELECT COUNT(*)
        FROM files f
        WHERE f.fk_books = b.pk AND f.obsoleted = 0 AND f.diskstatus = 0
    )::int AS file_count,
    jsonb_build_object(
        -- ========================================================================
        -- DUBLIN CORE: Identifier
        -- ========================================================================
        'identifier', b.pk,
        
        -- ========================================================================
        -- DUBLIN CORE: Title (MARC 245)
        -- ========================================================================
        'title', b.title,
        
        -- Full MARC 245 with subtitle parsing (splits on $b)
        'titleData', (
            SELECT jsonb_build_object(
                'full', a.text,
                'title', CASE 
                    WHEN a.text LIKE '%$b%' THEN RTRIM(SPLIT_PART(a.text, '$b', 1), ' :')
                    ELSE a.text
                END,
                'subtitle', CASE 
                    WHEN a.text LIKE '%$b%' THEN TRIM(SPLIT_PART(a.text, '$b', 2))
                    ELSE NULL
                END,
                'nonfiling', a.nonfiling
            )
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 245
            LIMIT 1
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Alternative Title (MARC 246)
        -- ========================================================================
        'alternative', (
            SELECT jsonb_agg(a.text ORDER BY a.pk)
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 246
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Alternative Title Variant (MARC 206)
        -- Maps to dc.alt_title in DublinCoreMapping.py
        -- ========================================================================
        'altTitle', (
            SELECT a.text 
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 206
            LIMIT 1
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Uniform Title (MARC 240)
        -- ========================================================================
        'uniformTitle', (
            SELECT a.text 
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 240
            LIMIT 1
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Creator/Contributors (with MARC relator codes)
        -- Maps to dc.authors in DublinCoreMapping.py
        -- ========================================================================
        'creators', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'id', au.pk,
                    'name', au.author,
                    'role', r.role,
                    'marcrel', r.pk,
                    'birthdate', au.born_floor,
                    'birthdate_ceil', au.born_ceil,
                    'deathdate', au.died_floor,
                    'deathdate_ceil', au.died_ceil,
                    'heading', mba.heading,
                    'aliases', COALESCE((
                        SELECT jsonb_agg(al.alias)
                        FROM aliases al
                        WHERE al.fk_authors = au.pk
                    ), '[]'::jsonb),
                    'webpages', COALESCE((
                        SELECT jsonb_agg(jsonb_build_object('url', aw.url, 'description', aw.description))
                        FROM author_urls aw
                        WHERE aw.fk_authors = au.pk
                    ), '[]'::jsonb)
                ) ORDER BY mba.heading, r.role, au.author
            )
            FROM mn_books_authors mba
            JOIN authors au ON mba.fk_authors = au.pk
            JOIN roles r ON mba.fk_roles = r.pk
            WHERE mba.fk_books = b.pk
        ), '[]'::jsonb),
        
        -- ========================================================================
        -- DUBLIN CORE: Subject (LCSH)
        -- Maps to dc.subjects in DublinCoreMapping.py
        -- ========================================================================
        'subjects', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('id', s.pk, 'subject', s.subject)
                ORDER BY s.subject
            )
            FROM mn_books_subjects mbs
            JOIN subjects s ON mbs.fk_subjects = s.pk
            WHERE mbs.fk_books = b.pk
        ), '[]'::jsonb),
        
        -- ========================================================================
        -- DUBLIN CORE: Description (MARC 500 - General Note)
        -- Maps to dc.notes in DublinCoreMapping.py
        -- ========================================================================
        'description', (
            SELECT jsonb_agg(a.text ORDER BY a.pk)
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 500
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Summary (MARC 520)
        -- ========================================================================
        'summary', (
            SELECT jsonb_agg(a.text ORDER BY a.pk)
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 520
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Table of Contents (MARC 505)
        -- Maps to dc.contents in DublinCoreMapping.py
        -- ========================================================================
        'tableOfContents', (
            SELECT a.text 
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 505
            LIMIT 1
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Publisher (MARC 260, 264)
        -- Maps to dc.pubinfo in DublinCoreMapping.py
        -- Parses $a (place), $b (publisher), $c (years) per parse260()
        -- ========================================================================
        'publisher', (
            SELECT jsonb_build_object(
                'raw', a.text,
                'place', CASE 
                    WHEN a.text LIKE '%$a%' THEN TRIM(BOTH ' :,.;[]' FROM SPLIT_PART(SPLIT_PART(a.text, '$a', 2), '$', 1))
                    ELSE NULL
                END,
                'publisher', CASE 
                    WHEN a.text LIKE '%$b%' THEN TRIM(BOTH ' :,.;[]' FROM SPLIT_PART(SPLIT_PART(a.text, '$b', 2), '$', 1))
                    ELSE NULL
                END,
                'years', CASE 
                    WHEN a.text LIKE '%$c%' THEN TRIM(SPLIT_PART(SPLIT_PART(a.text, '$c', 2), '$', 1))
                    ELSE NULL
                END
            )
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist IN (260, 264)
            ORDER BY a.fk_attriblist
            LIMIT 1
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Date
        -- Maps to dc.release_date in DublinCoreMapping.py
        -- ========================================================================
        'date', b.release_date,
        
        -- ========================================================================
        -- DUBLIN CORE: Type (DCMI Types)
        -- Maps to dc.dcmitypes in DublinCoreMapping.py
        -- ========================================================================
        'type', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('id', d.pk, 'dcmitype', d.dcmitype, 'description', d.description)
            )
            FROM mn_books_categories mbc
            JOIN dcmitypes d ON mbc.fk_categories = d.pk
            WHERE mbc.fk_books = b.pk
        ), '[{"id": 1, "dcmitype": "Text", "description": "Text"}]'::jsonb),
        
        -- ========================================================================
        -- DUBLIN CORE: Format (Files)
        -- Maps to dc.files in DublinCoreMapping.py
        -- ========================================================================
        'format', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'id', f.pk,
                    'filename', f.filename,
                    'filetype', f.fk_filetypes,
                    'hr_filetype', ft.filetype,
                    'mediatype', ft.mediatype,
                    'encoding', f.fk_encodings,
                    'compression', f.fk_compressions,
                    'extent', f.filesize,
                    'modified', f.filemtime,
                    'generated', ft.generated
                ) ORDER BY ft.sortorder, f.fk_filetypes
            )
            FROM files f
            LEFT JOIN filetypes ft ON f.fk_filetypes = ft.pk
            WHERE f.fk_books = b.pk 
              AND f.obsoleted = 0 
              AND f.diskstatus = 0
        ), '[]'::jsonb),
        
        -- Physical Description (MARC 300)
        'physicalDescription', (
            SELECT a.text 
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 300
            LIMIT 1
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Language
        -- Maps to dc.languages in DublinCoreMapping.py
        -- ========================================================================
        'language', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('code', l.pk, 'name', l.lang)
            )
            FROM mn_books_langs mbl
            JOIN langs l ON mbl.fk_langs = l.pk
            WHERE mbl.fk_books = b.pk
        ), '[{"code": "en", "name": "English"}]'::jsonb),
        
        -- Language Note (MARC 546)
        'languageNote', (
            SELECT a.text 
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 546
            LIMIT 1
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Source (MARC 534 - Original Version Note)
        -- ========================================================================
        'source', (
            SELECT jsonb_agg(a.text ORDER BY a.pk)
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 534
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Relation (MARC 787 - Other Relationship Entry)
        -- ========================================================================
        'relation', (
            SELECT jsonb_agg(a.text ORDER BY a.pk)
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 787
        ),
        
        -- ========================================================================
        -- DUBLIN CORE: Coverage (LoCC)
        -- Maps to dc.loccs in DublinCoreMapping.py
        -- ========================================================================
        'coverage', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('id', lc.pk, 'locc', lc.locc)
            )
            FROM mn_books_loccs mblc
            JOIN loccs lc ON mblc.fk_loccs = lc.pk
            WHERE mblc.fk_books = b.pk
        ), '[]'::jsonb),
        
        -- ========================================================================
        -- DUBLIN CORE: Rights
        -- Maps to dc.rights in DublinCoreMapping.py
        -- ========================================================================
        'rights', CASE 
            WHEN b.copyrighted = 1 THEN 'Copyrighted. Read the copyright notice inside this book for details.'
            ELSE 'Public domain in the USA.'
        END,
        
        -- ========================================================================
        -- PG EXTENSIONS: Bookshelves
        -- Maps to dc.bookshelves in DublinCoreMapping.py
        -- ========================================================================
        'bookshelves', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('id', bs.pk, 'bookshelf', bs.bookshelf)
                ORDER BY bs.bookshelf
            )
            FROM mn_books_bookshelves mbbs
            JOIN bookshelves bs ON mbbs.fk_bookshelves = bs.pk
            WHERE mbbs.fk_books = b.pk
        ), '[]'::jsonb),
        
        -- ========================================================================
        -- PG EXTENSIONS: Credits (MARC 508)
        -- Maps to dc.credit in DublinCoreMapping.py
        -- ========================================================================
        'credits', (
            SELECT jsonb_agg(
                TRIM(
                    CASE 
                        WHEN a.text ~* '\s*updated?:\s*' 
                        THEN (regexp_split_to_array(a.text, '\s*[Uu][Pp][Dd][Aa][Tt][Ee][Dd]?:\s*'))[1]
                        ELSE a.text
                    END
                )
                ORDER BY a.pk
            )
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 508
        ),
        
        -- ========================================================================
        -- PG EXTENSIONS: Cover Page (MARC 901)
        -- ========================================================================
        'coverpage', (
            SELECT jsonb_agg(a.text ORDER BY a.pk)
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 901
        ),
        
        -- ========================================================================
        -- PG EXTENSIONS: Downloads
        -- ========================================================================
        'downloads', b.downloads,
        
        -- ========================================================================
        -- PG EXTENSIONS: Project Gutenberg specific fields (MARC 904-907)
        -- ========================================================================
        'scanUrls', (
            SELECT a.text 
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 904
            LIMIT 1
        ),
        
        'requestKey', (
            SELECT a.text 
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 905
            LIMIT 1
        ),
        
        'pubInfo906', (
            SELECT jsonb_build_object(
                'raw', a.text,
                'firstYear', CASE 
                    WHEN a.text NOT LIKE '%$b%' THEN a.text
                    ELSE SPLIT_PART(a.text, '$b', 1)
                END,
                'publisher', CASE 
                    WHEN a.text LIKE '%$b%' THEN SPLIT_PART(SPLIT_PART(a.text, '$b', 2), ',', 1)
                    ELSE NULL
                END
            )
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 906
            LIMIT 1
        ),
        
        'pubCountry', (
            SELECT a.text 
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 907
            LIMIT 1
        ),
        
        -- ========================================================================
        -- COMPLETE MARC ATTRIBUTES
        -- ========================================================================
        'marc', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'code', al.pk,
                    'name', al.name,
                    'caption', al.caption,
                    'text', a.text,
                    'nonfiling', a.nonfiling
                ) ORDER BY al.pk, a.pk
            )
            FROM attributes a
            JOIN attriblist al ON a.fk_attriblist = al.pk
            WHERE a.fk_books = b.pk
        ), '[]'::jsonb)
        
    ) AS dc
FROM books b;

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Primary key
CREATE UNIQUE INDEX idx_mv_dc_pk ON mv_books_dc (book_id);

-- B-tree: equality, range, ORDER BY
CREATE INDEX idx_mv_dc_downloads ON mv_books_dc (downloads DESC);
CREATE INDEX idx_mv_dc_release ON mv_books_dc (release_date DESC);
CREATE INDEX idx_mv_dc_lang ON mv_books_dc (primary_language);
CREATE INDEX idx_mv_dc_locc ON mv_books_dc (primary_locc);
CREATE INDEX idx_mv_dc_dcmitype ON mv_books_dc (primary_dcmitype);
CREATE INDEX idx_mv_dc_birth ON mv_books_dc (author_birthdate) WHERE author_birthdate IS NOT NULL;
CREATE INDEX idx_mv_dc_death ON mv_books_dc (author_deathdate) WHERE author_deathdate IS NOT NULL;

-- GIN: full-text search (tsvector)
CREATE INDEX idx_mv_dc_tsvec ON mv_books_dc USING GIN (tsvec);
CREATE INDEX idx_mv_dc_author_tsvec ON mv_books_dc USING GIN (author_tsvec);
CREATE INDEX idx_mv_dc_subject_tsvec ON mv_books_dc USING GIN (subject_tsvec);
CREATE INDEX idx_mv_dc_bookshelf_tsvec ON mv_books_dc USING GIN (bookshelf_tsvec);

-- GIN: trigram (title, author, subject)
CREATE INDEX idx_mv_dc_title_trgm ON mv_books_dc USING GIN (title gin_trgm_ops);
CREATE INDEX idx_mv_dc_author_trgm ON mv_books_dc USING GIN (primary_author gin_trgm_ops);
CREATE INDEX idx_mv_dc_subject_trgm ON mv_books_dc USING GIN (primary_subject gin_trgm_ops);

-- Partial: common filtered subsets
CREATE INDEX idx_mv_dc_pd ON mv_books_dc (downloads DESC) WHERE copyrighted = 0;
CREATE INDEX idx_mv_dc_audio ON mv_books_dc (downloads DESC) WHERE is_audiobook = true;
CREATE INDEX idx_mv_dc_files ON mv_books_dc (downloads DESC) WHERE has_files = true;

-- GIN JSONB: nested array searches
CREATE INDEX idx_mv_dc_subjects ON mv_books_dc USING GIN ((dc->'subjects') jsonb_path_ops);
CREATE INDEX idx_mv_dc_bookshelves ON mv_books_dc USING GIN ((dc->'bookshelves') jsonb_path_ops);
CREATE INDEX idx_mv_dc_creators ON mv_books_dc USING GIN ((dc->'creators') jsonb_path_ops);

COMMIT;