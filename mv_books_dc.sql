-- Dublin Core Materialized View
-- Aligned with libgutenberg DublinCoreMapping.py

BEGIN;

-- Reasonable memory settings for MV build (adjust based on available RAM)
-- work_mem: per-operation memory (sorts, hashes) - don't go too high
-- maintenance_work_mem: for CREATE INDEX, VACUUM, etc.
SET LOCAL work_mem = '256MB';
SET LOCAL maintenance_work_mem = '1GB';
SET LOCAL max_parallel_workers_per_gather = 4;
SET LOCAL effective_io_concurrency = 200;  -- SSD optimization

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
    
    -- All authors sorted by heading then name (pipe-delimited for display)
    (
        SELECT STRING_AGG(au.author, ' | ' ORDER BY mba.heading, au.author)
        FROM mn_books_authors mba
        JOIN authors au ON mba.fk_authors = au.pk
        WHERE mba.fk_books = b.pk
    ) AS all_authors,
    
    -- All subjects sorted alphabetically (pipe-delimited for display)
    (
        SELECT STRING_AGG(s.subject, ' | ' ORDER BY s.subject)
        FROM mn_books_subjects mbs
        JOIN subjects s ON mbs.fk_subjects = s.pk
        WHERE mbs.fk_books = b.pk
    ) AS all_subjects,
    
    -- Combined searchable text: title + all authors + all subjects + all bookshelves
    -- (attributes excluded to reduce size for fuzzy search)
    CONCAT_WS(' ', 
        b.title,
        (SELECT STRING_AGG(au.author, ' ')
         FROM mn_books_authors mba
         JOIN authors au ON mba.fk_authors = au.pk
         WHERE mba.fk_books = b.pk),
        (SELECT STRING_AGG(s.subject, ' ')
         FROM mn_books_subjects mbs
         JOIN subjects s ON mbs.fk_subjects = s.pk
         WHERE mbs.fk_books = b.pk),
        (SELECT STRING_AGG(bs.bookshelf, ' ')
         FROM mn_books_bookshelves mbbs
         JOIN bookshelves bs ON mbbs.fk_bookshelves = bs.pk
         WHERE mbbs.fk_books = b.pk)
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

    (
        SELECT MAX(au.died_floor) 
        FROM mn_books_authors mba
        JOIN authors au ON mba.fk_authors = au.pk
        WHERE mba.fk_books = b.pk AND au.died_floor > 0
    ) AS max_author_deathyear,
    
    (
        SELECT MIN(au.died_floor) 
        FROM mn_books_authors mba
        JOIN authors au ON mba.fk_authors = au.pk
        WHERE mba.fk_books = b.pk AND au.died_floor > 0
    ) AS min_author_deathyear,
    
    -- LoCC codes as array for fast filtering
    COALESCE((
        SELECT ARRAY_AGG(lc.pk)
        FROM mn_books_loccs mblc
        JOIN loccs lc ON mblc.fk_loccs = lc.pk
        WHERE mblc.fk_books = b.pk
    ), ARRAY[]::text[]) AS locc_codes,
    
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
        'identifier', b.pk,
        'title', b.title,
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
        'alternative', (
            SELECT jsonb_agg(a.text ORDER BY a.pk)
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 246
        ),
        'altTitle', (
            SELECT a.text 
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 206
            LIMIT 1
        ),
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
        'subjects', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('id', s.pk, 'subject', s.subject)
                ORDER BY s.subject
            )
            FROM mn_books_subjects mbs
            JOIN subjects s ON mbs.fk_subjects = s.pk
            WHERE mbs.fk_books = b.pk
        ), '[]'::jsonb),
        'summary', (
            SELECT jsonb_agg(a.text ORDER BY a.pk)
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 520
        ),
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
        'date', b.release_date,
        'type', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('id', d.pk, 'dcmitype', d.dcmitype, 'description', d.description)
            )
            FROM mn_books_categories mbc
            JOIN dcmitypes d ON mbc.fk_categories = d.pk
            WHERE mbc.fk_books = b.pk
        ), '[{"id": 1, "dcmitype": "Text", "description": "Text"}]'::jsonb),
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
        'language', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('code', l.pk, 'name', l.lang)
            )
            FROM mn_books_langs mbl
            JOIN langs l ON mbl.fk_langs = l.pk
            WHERE mbl.fk_books = b.pk
        ), '[{"code": "en", "name": "English"}]'::jsonb),
        'source', (
            SELECT jsonb_agg(a.text ORDER BY a.pk)
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 534
        ),
        'relation', (
            SELECT jsonb_agg(a.text ORDER BY a.pk)
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 787
        ),
        'coverage', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('id', lc.pk, 'locc', lc.locc)
            )
            FROM mn_books_loccs mblc
            JOIN loccs lc ON mblc.fk_loccs = lc.pk
            WHERE mblc.fk_books = b.pk
        ), '[]'::jsonb),
        'rights', CASE 
            WHEN b.copyrighted = 1 THEN 'Copyrighted. Read the copyright notice inside this book for details.'
            ELSE 'Public domain in the USA.'
        END,
        'bookshelves', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('id', bs.pk, 'bookshelf', bs.bookshelf)
                ORDER BY bs.bookshelf
            )
            FROM mn_books_bookshelves mbbs
            JOIN bookshelves bs ON mbbs.fk_bookshelves = bs.pk
            WHERE mbbs.fk_books = b.pk
        ), '[]'::jsonb),
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
        'coverpage', (
            SELECT jsonb_agg(a.text ORDER BY a.pk)
            FROM attributes a 
            WHERE a.fk_books = b.pk AND a.fk_attriblist = 901
        ),
        'downloads', b.downloads,
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
CREATE UNIQUE INDEX idx_mv_pk ON mv_books_dc (book_id);

-- ============================================================================
-- B-TREE: Filtering & Sorting
-- ============================================================================
CREATE INDEX idx_mv_btree_downloads ON mv_books_dc (downloads DESC);
CREATE INDEX idx_mv_btree_copyrighted ON mv_books_dc (copyrighted);
CREATE INDEX idx_mv_btree_lang ON mv_books_dc (primary_lang);
CREATE INDEX idx_mv_btree_is_audio ON mv_books_dc (is_audio) WHERE is_audio = true;
CREATE INDEX idx_mv_btree_birthyear_max ON mv_books_dc (max_author_birthyear) WHERE max_author_birthyear IS NOT NULL;
CREATE INDEX idx_mv_btree_birthyear_min ON mv_books_dc (min_author_birthyear) WHERE min_author_birthyear IS NOT NULL;
CREATE INDEX idx_mv_btree_deathyear_max ON mv_books_dc (max_author_deathyear) WHERE max_author_deathyear IS NOT NULL;
CREATE INDEX idx_mv_btree_deathyear_min ON mv_books_dc (min_author_deathyear) WHERE min_author_deathyear IS NOT NULL;
CREATE INDEX idx_mv_btree_date ON mv_books_dc (text_to_date_immutable(dc->>'date'));

-- ============================================================================
-- GIN: Array containment (locc_codes)
-- ============================================================================
CREATE INDEX idx_mv_gin_locc ON mv_books_dc USING GIN (locc_codes);

-- ============================================================================
-- GIN: Full-text search (tsvector)
-- ============================================================================
CREATE INDEX idx_mv_fts_book ON mv_books_dc USING GIN (tsvec);
CREATE INDEX idx_mv_fts_title ON mv_books_dc USING GIN (title_tsvec);
CREATE INDEX idx_mv_fts_subtitle ON mv_books_dc USING GIN (subtitle_tsvec);
CREATE INDEX idx_mv_fts_author ON mv_books_dc USING GIN (author_tsvec);
CREATE INDEX idx_mv_fts_subject ON mv_books_dc USING GIN (subject_tsvec);
CREATE INDEX idx_mv_fts_bookshelf ON mv_books_dc USING GIN (bookshelf_tsvec);
CREATE INDEX idx_mv_fts_attribute ON mv_books_dc USING GIN (attribute_tsvec);

-- ============================================================================
-- GIN: Trigram ILIKE '%text%' (substring/contains search)
-- ============================================================================
CREATE INDEX idx_mv_contains_title ON mv_books_dc USING GIN (title gin_trgm_ops);
CREATE INDEX idx_mv_contains_subtitle ON mv_books_dc USING GIN (subtitle gin_trgm_ops);
CREATE INDEX idx_mv_contains_author ON mv_books_dc USING GIN (all_authors gin_trgm_ops);
CREATE INDEX idx_mv_contains_subject ON mv_books_dc USING GIN (all_subjects gin_trgm_ops);
CREATE INDEX idx_mv_contains_book ON mv_books_dc USING GIN (book_text gin_trgm_ops);
CREATE INDEX idx_mv_contains_bookshelf ON mv_books_dc USING GIN (bookshelf_text gin_trgm_ops);

-- ============================================================================
-- GiST: Trigram <% (fuzzy/typo-tolerant word similarity)
-- ============================================================================
CREATE INDEX idx_mv_fuzzy_title ON mv_books_dc USING GIST (title gist_trgm_ops);
CREATE INDEX idx_mv_fuzzy_subtitle ON mv_books_dc USING GIST (subtitle gist_trgm_ops);
CREATE INDEX idx_mv_fuzzy_author ON mv_books_dc USING GIST (all_authors gist_trgm_ops);
CREATE INDEX idx_mv_fuzzy_subject ON mv_books_dc USING GIST (all_subjects gist_trgm_ops);
CREATE INDEX idx_mv_fuzzy_book ON mv_books_dc USING GIST (book_text gist_trgm_ops);
CREATE INDEX idx_mv_fuzzy_bookshelf ON mv_books_dc USING GIST (bookshelf_text gist_trgm_ops);

-- ============================================================================
-- GIN: JSONB containment (@>) - ID lookups only
-- ============================================================================
CREATE INDEX idx_mv_jsonb_creators ON mv_books_dc USING GIN ((dc->'creators') jsonb_path_ops);
CREATE INDEX idx_mv_jsonb_subjects ON mv_books_dc USING GIN ((dc->'subjects') jsonb_path_ops);
CREATE INDEX idx_mv_jsonb_bookshelves ON mv_books_dc USING GIN ((dc->'bookshelves') jsonb_path_ops);

ANALYZE mv_books_dc;

-- ============================================================================
-- Refresh Function (for use with systemd timer or cron)
-- ============================================================================

CREATE OR REPLACE FUNCTION refresh_mv_books_dc()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Set memory for this session's refresh operation
    SET LOCAL work_mem = '256MB';
    SET LOCAL maintenance_work_mem = '1GB';
    
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_books_dc;
    ANALYZE mv_books_dc;
END;
$$;

-- To manually refresh: SELECT refresh_mv_books_dc();
-- For systemd timer, use: psql -U postgres -d your_database -c "SELECT refresh_mv_books_dc();"

COMMIT;