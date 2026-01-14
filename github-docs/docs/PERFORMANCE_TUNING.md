# Performance Tuning Guide

Comprehensive guide for optimizing the EVO data pipeline performance.

## Overview

The pipeline processes ~130M records across 4 entities. Performance optimizations have achieved **3-6x improvements** in processing time.

---

## Bottleneck Analysis

### Initial Performance Issues

| Bottleneck | Impact | Root Cause |
|------------|--------|------------|
| Database connections | 40% overhead | New connection per file |
| Individual INSERTs | 50-100x slower | Transaction per record |
| Index maintenance | 80% I/O | Updates during load |
| Memory usage | OOM errors | Loading full files |
| Failover handling | Job failures | No retry mechanism |

### Performance Profile (Before Optimization)

```
Sales Load (12.8M records):
├── Connection overhead: 35%
├── INSERT statements:   45%
├── Index updates:       15%
└── Network I/O:          5%
Total time: ~6 hours
```

---

## Optimization Strategies

### 1. Connection Pooling

**Problem**: Each file opened a new database connection.

**Solution**: `ThreadedConnectionPool` from psycopg2.

```python
from psycopg2 import pool

class ConnectionPoolManager:
    def __init__(self, min_conn=2, max_conn=10):
        self._pool = pool.ThreadedConnectionPool(
            min_conn, max_conn,
            host=PG_HOST, port=PG_PORT, database=PG_DATABASE,
            user=PG_USER, password=PG_PASSWORD, sslmode='require'
        )
    
    @contextmanager
    def get_connection(self):
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)
```

**Result**: 20-30% reduction in overhead.

### 2. COPY Command with Temp Tables

**Problem**: Individual INSERTs are extremely slow.

**Solution**: Bulk COPY to temp table, then UPSERT.

```python
def load_batch_with_copy(conn, records):
    cur = conn.cursor()
    
    # 1. Create temp table
    cur.execute("""
        CREATE TEMP TABLE tmp_load (
            id BIGINT,
            raw_data JSONB,
            source_file TEXT,
            run_id TEXT
        ) ON COMMIT DROP
    """)
    
    # 2. COPY data (50-100x faster than INSERT)
    buffer = io.StringIO()
    for rec in records:
        buffer.write(f"{rec['id']}\t{json.dumps(rec['data'])}\t{rec['file']}\t{rec['run']}\n")
    buffer.seek(0)
    
    cur.copy_from(buffer, 'tmp_load', sep='\t')
    
    # 3. UPSERT from temp
    cur.execute("""
        INSERT INTO target_table (id, raw_data, source_file, run_id)
        SELECT id, raw_data, source_file, run_id FROM tmp_load
        ON CONFLICT (id) DO UPDATE SET
            raw_data = EXCLUDED.raw_data,
            _updated_at = NOW()
    """)
    
    conn.commit()
```

**Benchmark**:

| Method | 1M Records | Relative |
|--------|------------|----------|
| Individual INSERT | 8 hours | 1x |
| Batch INSERT (1000) | 45 min | 10x |
| COPY + UPSERT | 5 min | **96x** |

### 3. Batch Processing

**Problem**: One file per COPY operation has overhead.

**Solution**: Group multiple files into single COPY.

```python
def process_batch(file_paths, batch_size=10):
    batches = [file_paths[i:i+batch_size] for i in range(0, len(file_paths), batch_size)]
    
    for batch in batches:
        all_records = []
        for path in batch:
            records = read_and_decompress(path)
            all_records.extend(records)
        
        load_batch_with_copy(conn, all_records)
```

**Result**: 2-3x reduction in connection/transaction overhead.

### 4. Full-Refresh Mode

**Problem**: Indexes slow down bulk loads.

**Solution**: For initial/full loads, disable indexes.

```python
def transform_full_refresh(conn):
    cur = conn.cursor()
    
    # 1. Drop indexes
    cur.execute("DROP INDEX IF EXISTS idx_members_branch")
    cur.execute("DROP INDEX IF EXISTS idx_members_status")
    
    # 2. Truncate target
    cur.execute("TRUNCATE TABLE core.evo_members CASCADE")
    conn.commit()
    
    # 3. Bulk INSERT (no conflict handling needed)
    cur.execute("""
        INSERT INTO core.evo_members (...)
        SELECT ... FROM stg_evo.members_raw
    """)
    conn.commit()
    
    # 4. Recreate indexes
    cur.execute("CREATE INDEX idx_members_branch ON core.evo_members(branch_id)")
    cur.execute("CREATE INDEX idx_members_status ON core.evo_members(status)")
    conn.commit()
```

**Result**: 5-10x faster for loads where >50% of data changes.

### 5. Streaming Decompression

**Problem**: Loading entire .gz files into memory causes OOM.

**Solution**: Stream decompress.

```python
import gzip

def stream_decompress(file_path):
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            yield json.loads(line)

# Process in chunks
def process_large_file(file_path, chunk_size=10000):
    chunk = []
    for record in stream_decompress(file_path):
        chunk.append(record)
        if len(chunk) >= chunk_size:
            load_batch_with_copy(conn, chunk)
            chunk = []
    
    if chunk:
        load_batch_with_copy(conn, chunk)
```

**Result**: 50% less memory usage, handles arbitrarily large files.

### 6. Exponential Backoff Retry

**Problem**: Azure PostgreSQL failovers cause job failures.

**Solution**: Retry with exponential backoff.

```python
def execute_with_retry(func, max_retries=5, base_delay=30):
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            error_msg = str(e).lower()
            is_failover = any(x in error_msg for x in [
                'read-only', 'readonly', 'connection already closed',
                'adminshutdown', 'server closed', 'terminating connection'
            ])
            
            if is_failover and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)  # 30, 60, 120, 240s
                logger.warning(f"Failover detected, waiting {delay}s...")
                time.sleep(delay)
            else:
                raise
```

**Result**: 100% reliability during Azure maintenance windows.

---

## Benchmark Results

### Before vs After

| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| Sales Load (12.8M) | 6h | 2h | **3x** |
| Sales Transform | 2h | 30min | **4x** |
| Members Load (2.4M) | 3h | 1h | **3x** |
| Members Transform | 4.6h | 45min | **6x** |
| Entries Load (~110M) | 15h | 5h | **3x** |

### Processing Rates

| Stage | Records/Second |
|-------|----------------|
| API Extraction | 140-220/s |
| Bronze → STG (COPY) | 500-600/s |
| STG → CORE Transform | 4,000-7,000/s |

### Time Distribution (Optimized)

```
Sales Load v2 (12.8M records):
├── Download from Azure:  35%
├── Decompress + Parse:   25%
├── COPY to temp table:   30%
└── UPSERT to target:     10%
Total time: ~2 hours
```

---

## Configuration Recommendations

### PostgreSQL Settings

```sql
-- Increase work memory for sorts
ALTER SYSTEM SET work_mem = '256MB';

-- More memory for maintenance operations
ALTER SYSTEM SET maintenance_work_mem = '1GB';

-- Faster autovacuum
ALTER SYSTEM SET autovacuum_vacuum_scale_factor = 0.1;

-- Apply changes
SELECT pg_reload_conf();
```

### Python Script Parameters

```bash
# Optimal for 8-core machine
--workers 8          # Match CPU cores
--batch-size 10      # 10 files per COPY operation

# For memory-constrained environments
--workers 4
--batch-size 5
```

### Azure PostgreSQL Tier Selection

| Volume | Recommended Tier | vCores | Storage |
|--------|------------------|--------|---------|
| <10M records | B1ms | 1 | 32 GB |
| 10-50M records | B2ms | 2 | 128 GB |
| 50-200M records | B2ms | 2 | 256 GB |
| >200M records | GP_Gen5_4 | 4 | 512 GB |

---

## Storage Optimization

### Index Bloat Problem

Repeated UPSERTs cause PostgreSQL indexes to bloat:

```sql
-- Check current bloat
SELECT 
    schemaname || '.' || relname AS table,
    pg_size_pretty(pg_total_relation_size(relid)) AS total,
    pg_size_pretty(pg_relation_size(relid)) AS data,
    pg_size_pretty(pg_indexes_size(relid)) AS indexes,
    ROUND(100.0 * pg_indexes_size(relid) / pg_total_relation_size(relid), 1) AS index_pct
FROM pg_stat_user_tables
WHERE schemaname IN ('stg_evo', 'core')
ORDER BY pg_total_relation_size(relid) DESC;
```

**Typical bloat scenario**:
```
Table: stg_evo.sales_raw
Total: 123 GB
Data: 20 GB
Indexes: 103 GB (84% bloat!)
```

### Solution: VACUUM + REINDEX

```sql
-- Reclaim dead tuple space
VACUUM (VERBOSE, ANALYZE) stg_evo.sales_raw;

-- Rebuild bloated indexes (online)
REINDEX INDEX CONCURRENTLY stg_evo.sales_raw_pkey;
```

**Expected savings**: 60-80 GB recovered.

### Automated Optimization

```bash
# Analysis only
python sql/optimize_storage.py --analyze

# Full optimization (run during maintenance window)
python sql/optimize_storage.py --vacuum --reindex
```

---

## When to Use Full-Refresh

| Scenario | Mode | Why |
|----------|------|-----|
| Initial load | `--full-refresh` | No existing data to preserve |
| Complete reload after error | `--full-refresh` | Start clean |
| >50% of records changed | `--full-refresh` | TRUNCATE faster than UPSERT |
| Daily incremental | Normal (UPSERT) | Only update changed records |
| Testing/development | `--full-refresh` | Fast iteration |

---

## Monitoring Queries

### Load Progress

```sql
-- Records loaded per run
SELECT 
    run_id,
    COUNT(*) AS records,
    MIN(_loaded_at) AS started,
    MAX(_loaded_at) AS finished
FROM stg_evo.members_raw
GROUP BY run_id
ORDER BY started DESC;
```

### Processing Rate

```sql
-- Records per minute in last hour
SELECT 
    DATE_TRUNC('minute', _loaded_at) AS minute,
    COUNT(*) AS records
FROM stg_evo.sales_raw
WHERE _loaded_at > NOW() - INTERVAL '1 hour'
GROUP BY 1
ORDER BY 1;
```

### Dead Tuples (Bloat Indicator)

```sql
SELECT 
    schemaname || '.' || relname AS table,
    n_dead_tup AS dead_tuples,
    n_live_tup AS live_tuples,
    ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS bloat_pct
FROM pg_stat_user_tables
WHERE schemaname IN ('stg_evo', 'core')
  AND n_dead_tup > 10000
ORDER BY n_dead_tup DESC;
```

---

## Troubleshooting Performance Issues

### Slow COPY Operations

1. Check I/O wait: `iostat -x 1`
2. Verify network latency to Azure
3. Consider increasing `--batch-size`

### High Memory Usage

1. Reduce `--workers` count
2. Reduce `--batch-size`
3. Enable streaming decompression

### Slow Transforms

1. Use `--full-refresh` for large changes
2. Check for missing indexes on JOIN columns
3. Verify `work_mem` setting

### Azure Failover Interruptions

1. Scripts v2 handle automatically
2. If using v1, manually re-run
3. Check Azure Service Health for planned maintenance
