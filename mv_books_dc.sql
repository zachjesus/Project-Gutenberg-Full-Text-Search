-- Dublin Core Materialized View
-- Aligned with libgutenberg DublinCoreMapping.py

BEGIN;

SET LOCAL work_mem = '8GB';
SET LOCAL maintenance_work_mem = '10GB';
SET LOCAL max_parallel_workers_per_gather = 8;

SET LOCAL client_min_messages = WARNING;

CREATE OR REPLACE FUNCTION text_to_date_immutable(text) RETURNS date AS $$
    SELECT $1::date;
$$ LANGUAGE SQL IMMUTABLE STRICT;

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
    b.tsvec,      
    b.downloads,
    b.copyrighted,
    
    CONCAT_WS(' ', 
        b.title,
        (SELECT STRING_AGG(au.author, ' ')
         FROM mn_books_authors mba
         JOIN authors au ON mba.fk_authors = au.pk
         WHERE mba.fk_books = b.pk),
        (SELECT STRING_AGG(s.subject, ' ')
         FROM mn_books_subjects mbs
         JOIN subjects s ON mbs.fk_subjects = s.pk
         WHERE mbs.fk_books = b.pk)
    ) AS book_text,
    
    COALESCE((
        SELECT l.pk
        FROM mn_books_langs mbl
        JOIN langs l ON mbl.fk_langs = l.pk
        WHERE mbl.fk_books = b.pk
        LIMIT 1
    ), 'en') AS primary_lang,
    
    EXISTS (
        SELECT 1 FROM mn_books_categories mbc
        WHERE mbc.fk_books = b.pk AND mbc.fk_categories IN (1, 2)
    ) AS is_audio,
    
    EXISTS (
        SELECT 1 FROM files f
        WHERE f.fk_books = b.pk AND f.obsoleted = 0 AND f.diskstatus = 0
    ) AS has_files,
    
    EXISTS (
        SELECT 1 FROM attributes a 
        WHERE a.fk_books = b.pk AND a.fk_attriblist = 901
    ) AS has_cover,
    
    (
        SELECT CASE 
            WHEN a.text LIKE '%$b%' THEN 
                TRIM(BOTH ' :;,.' FROM SPLIT_PART(SPLIT_PART(a.text, '$b', 2), '$', 1))
            ELSE NULL
        END
        FROM attributes a 
        WHERE a.fk_books = b.pk AND a.fk_attriblist = 245
        LIMIT 1
    ) AS subtitle,
    
    (
        SELECT MAX(au.born_floor) 
        FROM mn_books_authors mba
        JOIN authors au ON mba.fk_authors = au.pk
        WHERE mba.fk_books = b.pk AND au.born_floor > 0
    ) AS max_author_birthyear,
    
    (
        SELECT MIN(au.born_floor) 
        FROM mn_books_authors mba
        JOIN authors au ON mba.fk_authors = au.pk
        WHERE mba.fk_books = b.pk AND au.born_floor > 0
    ) AS min_author_birthyear,
    
    -- Reuse existing tsvec from authors table (already indexed there)
    COALESCE((
        SELECT tsvector_agg(au.tsvec)
        FROM mn_books_authors mba
        JOIN authors au ON mba.fk_authors = au.pk
        WHERE mba.fk_books = b.pk AND au.tsvec IS NOT NULL
    ), ''::tsvector) AS author_tsvec,
    
    COALESCE((
        SELECT tsvector_agg(s.tsvec)
        FROM mn_books_subjects mbs
        JOIN subjects s ON mbs.fk_subjects = s.pk
        WHERE mbs.fk_books = b.pk AND s.tsvec IS NOT NULL
    ), ''::tsvector) AS subject_tsvec,
    
    COALESCE((
        SELECT tsvector_agg(bs.tsvec)
        FROM mn_books_bookshelves mbbs
        JOIN bookshelves bs ON mbbs.fk_bookshelves = bs.pk
        WHERE mbbs.fk_books = b.pk AND bs.tsvec IS NOT NULL
    ), ''::tsvector) AS bookshelf_tsvec,
    
    COALESCE((
        SELECT tsvector_agg(a.tsvec)
        FROM attributes a
        WHERE a.fk_books = b.pk AND a.tsvec IS NOT NULL
    ), ''::tsvector) AS attribute_tsvec,
    
    (
        SELECT au.author
        FROM mn_books_authors mba
        JOIN authors au ON mba.fk_authors = au.pk
        WHERE mba.fk_books = b.pk
        ORDER BY mba.heading
        LIMIT 1
    ) AS primary_author,
    
    (
        SELECT s.subject
        FROM mn_books_subjects mbs
        JOIN subjects s ON mbs.fk_subjects = s.pk
        WHERE mbs.fk_books = b.pk
        ORDER BY s.subject
        LIMIT 1
    ) AS primary_subject,
    
    -- Bookshelf text for fuzzy/contains search
    (
        SELECT STRING_AGG(bs.bookshelf, ' ')
        FROM mn_books_bookshelves mbbs
        JOIN bookshelves bs ON mbbs.fk_bookshelves = bs.pk
        WHERE mbbs.fk_books = b.pk
    ) AS bookshelf_text,
    
    -- Attribute text for fuzzy/contains search (all MARC field text)
    -- Strip MARC subfield delimiters ($a, $b, $c, etc.)
    (
        SELECT STRING_AGG(
            REGEXP_REPLACE(a.text, '\$[a-z0-9]', ' ', 'gi'),
            ' '
        )
        FROM attributes a
        WHERE a.fk_books = b.pk
    ) AS attribute_text,
    
    -- Title tsvec (FTS on title alone)
    to_tsvector('english', COALESCE(b.title, '')) AS title_tsvec,
    
    -- Subtitle tsvec (FTS on subtitle alone)
    to_tsvector('english', COALESCE((
        SELECT CASE 
            WHEN a.text LIKE '%$b%' THEN TRIM(SPLIT_PART(a.text, '$b', 2))
            ELSE NULL
        END
        FROM attributes a 
        WHERE a.fk_books = b.pk AND a.fk_attriblist = 245
        LIMIT 1
    ), '')) AS subtitle_tsvec,

    -- Everything else in JSONB
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

-- B-tree: ORDER BY / range filters
CREATE INDEX idx_mv_dc_downloads ON mv_books_dc (downloads DESC);

-- B-tree: top-level filter columns
CREATE INDEX idx_mv_dc_copyrighted ON mv_books_dc (copyrighted);
CREATE INDEX idx_mv_dc_lang ON mv_books_dc (primary_lang);
CREATE INDEX idx_mv_dc_is_audio ON mv_books_dc (is_audio);
CREATE INDEX idx_mv_dc_has_files ON mv_books_dc (has_files) WHERE has_files = true;
CREATE INDEX idx_mv_dc_has_cover ON mv_books_dc (has_cover) WHERE has_cover = true;
CREATE INDEX idx_mv_dc_max_birthyear ON mv_books_dc (max_author_birthyear) WHERE max_author_birthyear IS NOT NULL;
CREATE INDEX idx_mv_dc_min_birthyear ON mv_books_dc (min_author_birthyear) WHERE min_author_birthyear IS NOT NULL;

-- ============================================================================
-- GIN: Full-text search (tsvector)
-- ============================================================================
CREATE INDEX idx_mv_dc_tsvec ON mv_books_dc USING GIN (tsvec);
CREATE INDEX idx_mv_dc_title_tsvec ON mv_books_dc USING GIN (title_tsvec);
CREATE INDEX idx_mv_dc_subtitle_tsvec ON mv_books_dc USING GIN (subtitle_tsvec);
CREATE INDEX idx_mv_dc_author_tsvec ON mv_books_dc USING GIN (author_tsvec);
CREATE INDEX idx_mv_dc_subject_tsvec ON mv_books_dc USING GIN (subject_tsvec);
CREATE INDEX idx_mv_dc_bookshelf_tsvec ON mv_books_dc USING GIN (bookshelf_tsvec);
CREATE INDEX idx_mv_dc_attribute_tsvec ON mv_books_dc USING GIN (attribute_tsvec);

-- ============================================================================
-- GIN: Trigram ILIKE '%text%' (substring/contains search)
-- ============================================================================
CREATE INDEX idx_mv_dc_title_trgm ON mv_books_dc USING GIN (title gin_trgm_ops);
CREATE INDEX idx_mv_dc_subtitle_trgm ON mv_books_dc USING GIN (subtitle gin_trgm_ops);
CREATE INDEX idx_mv_dc_author_trgm ON mv_books_dc USING GIN (primary_author gin_trgm_ops);
CREATE INDEX idx_mv_dc_subject_trgm ON mv_books_dc USING GIN (primary_subject gin_trgm_ops);
CREATE INDEX idx_mv_dc_book_text_trgm ON mv_books_dc USING GIN (book_text gin_trgm_ops);
CREATE INDEX idx_mv_dc_bookshelf_text_trgm ON mv_books_dc USING GIN (bookshelf_text gin_trgm_ops);
CREATE INDEX idx_mv_dc_attribute_text_trgm ON mv_books_dc USING GIN (attribute_text gin_trgm_ops);

-- ============================================================================
-- GiST: Trigram <% (fuzzy/typo-tolerant word similarity)
-- ============================================================================
CREATE INDEX idx_mv_dc_title_trgm_gist ON mv_books_dc USING GIST (title gist_trgm_ops);
CREATE INDEX idx_mv_dc_subtitle_trgm_gist ON mv_books_dc USING GIST (subtitle gist_trgm_ops);
CREATE INDEX idx_mv_dc_author_trgm_gist ON mv_books_dc USING GIST (primary_author gist_trgm_ops);
CREATE INDEX idx_mv_dc_subject_trgm_gist ON mv_books_dc USING GIST (primary_subject gist_trgm_ops);
CREATE INDEX idx_mv_dc_book_text_trgm_gist ON mv_books_dc USING GIST (book_text gist_trgm_ops);
CREATE INDEX idx_mv_dc_bookshelf_text_trgm_gist ON mv_books_dc USING GIST (bookshelf_text gist_trgm_ops);
CREATE INDEX idx_mv_dc_attribute_text_trgm_gist ON mv_books_dc USING GIST (attribute_text gist_trgm_ops);

-- ============================================================================
-- GIN: JSONB containment (@>)
-- ============================================================================
CREATE INDEX idx_mv_dc_coverage ON mv_books_dc USING GIN ((dc->'coverage') jsonb_path_ops);
CREATE INDEX idx_mv_dc_bookshelves ON mv_books_dc USING GIN ((dc->'bookshelves') jsonb_path_ops);
CREATE INDEX idx_mv_dc_subjects ON mv_books_dc USING GIN ((dc->'subjects') jsonb_path_ops);
CREATE INDEX idx_mv_dc_format ON mv_books_dc USING GIN ((dc->'format') jsonb_path_ops);
CREATE INDEX idx_mv_dc_creators ON mv_books_dc USING GIN ((dc->'creators') jsonb_path_ops);

-- B-tree: date range filters
CREATE INDEX idx_mv_dc_date ON mv_books_dc (text_to_date_immutable(dc->>'date'));

ANALYZE mv_books_dc;

COMMIT;