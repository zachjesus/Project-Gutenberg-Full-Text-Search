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

-- Touch each GIN index with a dummy query to warm internal structures
SELECT COUNT(*) FROM mv_books_dc WHERE dc->'format' @> '[{"encoding": "utf-8"}]'::jsonb;
SELECT COUNT(*) FROM mv_books_dc WHERE dc->'creators' @> '[{"id": 1}]'::jsonb;
SELECT COUNT(*) FROM mv_books_dc WHERE dc->'subjects' @> '[{"id": 1}]'::jsonb;
SELECT COUNT(*) FROM mv_books_dc WHERE dc->'bookshelves' @> '[{"id": 1}]'::jsonb;
SELECT COUNT(*) FROM mv_books_dc WHERE dc->'coverage' @> '[{"id": "PS"}]'::jsonb;

-- Show cache status
SELECT 
    'mv_books_dc' as relation,
    pg_size_pretty(pg_relation_size('mv_books_dc')) as data_size,
    pg_size_pretty(pg_indexes_size('mv_books_dc')) as index_size,
    pg_size_pretty(pg_total_relation_size('mv_books_dc')) as total_size;

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