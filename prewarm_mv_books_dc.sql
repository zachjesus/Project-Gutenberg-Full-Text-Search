-- Prewarm mv_books_dc and all indexes into shared_buffers

CREATE EXTENSION IF NOT EXISTS pg_prewarm;

DO $$
DECLARE
    r RECORD;
    blocks BIGINT;
    total_blocks BIGINT := 0;
BEGIN
    -- Prewarm the MV itself (all data pages)
    blocks := pg_prewarm('mv_books_dc', 'buffer', 'main');
    total_blocks := total_blocks + blocks;
    RAISE NOTICE 'mv_books_dc (data): % blocks', blocks;
    
    -- Prewarm TOAST table (large JSONB values stored separately)
    BEGIN
        blocks := pg_prewarm(
            (SELECT reltoastrelid::regclass FROM pg_class WHERE relname = 'mv_books_dc'),
            'buffer', 'main'
        );
        total_blocks := total_blocks + blocks;
        RAISE NOTICE 'mv_books_dc (toast): % blocks', blocks;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'mv_books_dc (toast): skipped (no toast table)';
    END;
    
    -- Prewarm all indexes
    FOR r IN 
        SELECT indexname FROM pg_indexes WHERE tablename = 'mv_books_dc'
    LOOP
        blocks := pg_prewarm(r.indexname::regclass, 'buffer', 'main');
        total_blocks := total_blocks + blocks;
        RAISE NOTICE '%: % blocks', r.indexname, blocks;
    END LOOP;
    
    RAISE NOTICE '-----------------------------------';
    RAISE NOTICE 'Total: % blocks (% MB)', total_blocks, (total_blocks * 8 / 1024);
END $$;

-- Force a sequential scan to warm OS page cache too
SELECT COUNT(*) FROM mv_books_dc WHERE dc IS NOT NULL;

-- ============================================================================
-- Touch GIN JSONB indexes (ID lookups only)
-- ============================================================================
SELECT COUNT(*) FROM mv_books_dc WHERE dc->'creators' @> '[{"id": 1}]'::jsonb;
SELECT COUNT(*) FROM mv_books_dc WHERE dc->'subjects' @> '[{"id": 1}]'::jsonb;
SELECT COUNT(*) FROM mv_books_dc WHERE dc->'bookshelves' @> '[{"id": 1}]'::jsonb;

-- ============================================================================
-- Touch GIN array index (locc_codes)
-- ============================================================================
SELECT COUNT(*) FROM mv_books_dc WHERE 'PS' = ANY(locc_codes);

-- ============================================================================
-- Touch GIN tsvector indexes (FTS)
-- ============================================================================
SELECT COUNT(*) FROM mv_books_dc WHERE tsvec @@ to_tsquery('english', 'shakespeare');
SELECT COUNT(*) FROM mv_books_dc WHERE title_tsvec @@ to_tsquery('english', 'adventure');
SELECT COUNT(*) FROM mv_books_dc WHERE subtitle_tsvec @@ to_tsquery('english', 'volume');
SELECT COUNT(*) FROM mv_books_dc WHERE author_tsvec @@ to_tsquery('english', 'twain');
SELECT COUNT(*) FROM mv_books_dc WHERE subject_tsvec @@ to_tsquery('english', 'fiction');
SELECT COUNT(*) FROM mv_books_dc WHERE bookshelf_tsvec @@ to_tsquery('english', 'science');
SELECT COUNT(*) FROM mv_books_dc WHERE attribute_tsvec @@ to_tsquery('english', 'illustrated');

-- ============================================================================
-- Touch GIN trigram indexes (ILIKE substring/contains)
-- ============================================================================
SELECT COUNT(*) FROM mv_books_dc WHERE title ILIKE '%adventure%';
SELECT COUNT(*) FROM mv_books_dc WHERE subtitle ILIKE '%volume%';
SELECT COUNT(*) FROM mv_books_dc WHERE all_authors ILIKE '%twain%';
SELECT COUNT(*) FROM mv_books_dc WHERE all_subjects ILIKE '%fiction%';
SELECT COUNT(*) FROM mv_books_dc WHERE book_text ILIKE '%shakespeare%';
SELECT COUNT(*) FROM mv_books_dc WHERE bookshelf_text ILIKE '%science%';

-- ============================================================================
-- Touch GiST trigram indexes (fuzzy/typo-tolerant)
-- ============================================================================
SELECT COUNT(*) FROM mv_books_dc WHERE 'shakspeare' <% title;
SELECT COUNT(*) FROM mv_books_dc WHERE 'volumee' <% subtitle;
SELECT COUNT(*) FROM mv_books_dc WHERE 'twian' <% all_authors;
SELECT COUNT(*) FROM mv_books_dc WHERE 'ficton' <% all_subjects;
SELECT COUNT(*) FROM mv_books_dc WHERE 'shakspere' <% book_text;
SELECT COUNT(*) FROM mv_books_dc WHERE 'scince' <% bookshelf_text;

-- ============================================================================
-- Touch B-tree indexes
-- ============================================================================
SELECT COUNT(*) FROM mv_books_dc WHERE downloads > 1000;
SELECT COUNT(*) FROM mv_books_dc WHERE copyrighted = 0;
SELECT COUNT(*) FROM mv_books_dc WHERE lang_codes @> ARRAY['en'];
SELECT COUNT(*) FROM mv_books_dc WHERE is_audio = true;
SELECT COUNT(*) FROM mv_books_dc WHERE max_author_birthyear > 1000;
SELECT COUNT(*) FROM mv_books_dc WHERE min_author_birthyear < 2000;
SELECT COUNT(*) FROM mv_books_dc WHERE max_author_deathyear > 1000;
SELECT COUNT(*) FROM mv_books_dc WHERE min_author_deathyear < 2000;
SELECT COUNT(*) FROM mv_books_dc WHERE text_to_date_immutable(dc->>'date') > '2020-01-01'::date;

-- ============================================================================
-- Show cache status
-- ============================================================================
SELECT 
    'mv_books_dc' as relation,
    pg_size_pretty(pg_relation_size('mv_books_dc')) as data_size,
    pg_size_pretty(pg_indexes_size('mv_books_dc')) as index_size,
    pg_size_pretty(pg_total_relation_size('mv_books_dc')) as total_size;

-- Show individual index sizes
SELECT 
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as size
FROM pg_indexes 
WHERE tablename = 'mv_books_dc'
ORDER BY pg_relation_size(indexname::regclass) DESC;

-- Show buffer cache hit ratio (run after some queries)
SELECT 
    schemaname, relname,
    heap_blks_read, heap_blks_hit,
    CASE WHEN heap_blks_hit + heap_blks_read > 0 
        THEN round(100.0 * heap_blks_hit / (heap_blks_hit + heap_blks_read), 2)
        ELSE 0 
    END as hit_ratio_pct
FROM pg_statio_user_tables 
WHERE relname = 'mv_books_dc';