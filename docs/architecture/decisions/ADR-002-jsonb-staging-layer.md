# ADR-002: JSONB Staging Layer for Schema Flexibility

## Status
**Accepted** - January 2026

## Context

SaaS APIs (Pipedrive, Zendesk, EVO) frequently change their response schemas:
- New fields added without notice
- Field types changed (string → object)
- Nested structures modified
- Fields deprecated or renamed

Traditional ETL with fixed schemas would break on any schema change, requiring:
- Code changes
- Schema migrations
- Potential data loss

### The Challenge

```python
# Day 1: API returns
{"id": 1, "value": 1000}

# Day 30: API now returns (without warning)
{"id": 1, "value": {"amount": 1000, "currency": "BRL"}}

# Fixed-schema ETL: 💥 FAILS
# JSONB ETL: ✅ WORKS (stores raw, extracts later)
```

## Decision

Implement a **two-phase transformation**:

1. **Silver Layer (STG)**: Store raw API responses as JSONB
2. **Gold Layer (CORE)**: Extract and type-cast to normalized tables

```
API Response → Bronze (.jsonl.gz) → Silver (JSONB) → Gold (typed columns)
```

## Implementation

### Silver Layer Schema

```sql
CREATE TABLE stg_pipedrive.deals_raw (
    id SERIAL PRIMARY KEY,
    payload JSONB NOT NULL,           -- Raw API response
    scope VARCHAR(50) NOT NULL,       -- Multi-tenant identifier
    source_blob_path TEXT NOT NULL,   -- Lineage to Bronze
    source_line_no INTEGER NOT NULL,  -- Exact record location
    run_id VARCHAR(20) NOT NULL,      -- Batch identifier
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE (scope, source_blob_path, source_line_no)
);
```

### Gold Layer Extraction

```sql
-- Extract with safe type casting
INSERT INTO core.pd_deals (deal_id, scope, title, value, ...)
SELECT 
    (payload->>'id')::BIGINT,
    scope,
    payload->>'title',
    -- Handle both old and new value formats
    CASE 
        WHEN jsonb_typeof(payload->'value') = 'object' 
        THEN (payload->'value'->>'amount')::NUMERIC
        ELSE (payload->>'value')::NUMERIC
    END,
    ...
FROM stg_pipedrive.deals_raw;
```

## Rationale

### Benefits

| Benefit | Description |
|---------|-------------|
| **Schema Evolution** | New fields automatically captured, no code changes |
| **Backward Compatibility** | Old data preserved, can extract new fields retroactively |
| **Debugging** | Full original payload available for troubleshooting |
| **Audit Trail** | Can compare API responses over time |
| **Graceful Degradation** | Pipeline doesn't break on API changes |

### Example: Handling Unknown Fields

```sql
-- Extract known fields to columns
SELECT payload->>'title' as title FROM stg_pipedrive.deals_raw;

-- Unknown fields preserved in custom_fields JSONB column
SELECT payload - ARRAY['id','title','value'] as custom_fields 
FROM stg_pipedrive.deals_raw;
```

## Trade-offs

| Trade-off | Mitigation |
|-----------|------------|
| Larger storage (JSONB overhead) | Compression, TOAST, ~20% overhead acceptable |
| Slower queries on raw data | Gold layer provides typed, indexed access |
| More complex extraction logic | Encapsulated in transformer modules |
| Two-step transformation | Clear separation of concerns |

## Storage Impact

| Layer | Format | Size (250k records) |
|-------|--------|---------------------|
| Bronze | .jsonl.gz | ~50 MB |
| Silver | JSONB | ~200 MB |
| Gold | Typed columns | ~150 MB |

Total: ~400 MB - well within PostgreSQL capabilities.

## Consequences

### Positive
- Zero pipeline breaks from API schema changes (since implementation)
- Ability to backfill new fields from historical data
- Complete audit trail of raw data
- Simplified error handling (store first, validate later)

### Negative
- Queries on Silver layer are slower (JSONB extraction)
- Need to maintain two layers (Silver + Gold)
- Gold layer transformers require careful NULL handling

## References

- [PostgreSQL JSONB Documentation](https://www.postgresql.org/docs/current/datatype-json.html)
- [Schema Evolution Best Practices](https://www.confluent.io/blog/schemas-contracts-compatibility/)
