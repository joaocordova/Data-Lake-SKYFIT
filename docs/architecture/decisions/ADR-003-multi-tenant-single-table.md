# ADR-003: Multi-Tenant Single Table Design

## Status
**Accepted** - January 2026

## Context

Pipedrive CRM is used by two business units at SkyFit:
- **Comercial**: Main sales team (~30k deals)
- **Expansão**: Franchise expansion team (~350 deals)

Each unit has its own Pipedrive account with separate API tokens. We need to decide how to store this data.

### Options Considered

#### Option A: Separate Tables per Scope
```sql
core.pd_deals_comercial (deal_id PK, ...)
core.pd_deals_expansao (deal_id PK, ...)
```

#### Option B: Single Table with Scope Column (Chosen)
```sql
core.pd_deals (deal_id, scope, ..., PRIMARY KEY (deal_id, scope))
```

## Decision

Use **single table with composite primary key** including scope.

## Implementation

### Schema Design

```sql
CREATE TABLE core.pd_deals (
    deal_id BIGINT NOT NULL,
    scope VARCHAR(50) NOT NULL,  -- 'comercial' or 'expansao'
    
    -- Business columns
    title VARCHAR(500),
    value NUMERIC(15,2),
    status VARCHAR(20),
    ...
    
    -- Composite primary key
    PRIMARY KEY (deal_id, scope)
);

-- Index for scope-filtered queries
CREATE INDEX idx_pd_deals_scope ON core.pd_deals(scope);
```

### Convenience Views

```sql
-- For users who only need one scope
CREATE VIEW core.vw_pd_deals_comercial AS 
SELECT * FROM core.pd_deals WHERE scope = 'comercial';

CREATE VIEW core.vw_pd_deals_expansao AS 
SELECT * FROM core.pd_deals WHERE scope = 'expansao';
```

## Rationale

### Comparison Matrix

| Factor | Separate Tables | Single Table |
|--------|-----------------|--------------|
| **Cross-scope queries** | Complex UNION ALL | Simple GROUP BY |
| **Schema maintenance** | 2x DDL changes | 1x DDL changes |
| **Code complexity** | Dynamic table names | Fixed table names |
| **Performance** | Better isolation | Good with indexes |
| **Adding new scope** | New migration | Just add data |
| **Power BI** | Multiple datasets | Single dataset |

### Query Examples

```sql
-- Cross-scope comparison (easy with single table)
SELECT 
    scope,
    COUNT(*) as deals,
    SUM(CASE WHEN status = 'won' THEN 1 END) as won,
    AVG(value) as avg_value
FROM core.pd_deals
GROUP BY scope;

-- Results:
-- scope      | deals  | won    | avg_value
-- comercial  | 30,636 | 10,234 | 5,420.00
-- expansao   |    352 |    156 | 8,750.00
```

```sql
-- Scope isolation (when needed)
SELECT * FROM core.vw_pd_deals_comercial WHERE status = 'open';
```

### Why Not Separate Tables?

1. **Schema Drift**: Maintaining identical schemas across tables is error-prone
2. **Code Duplication**: ETL would need dynamic table selection
3. **Analytics Friction**: Every cross-scope query needs UNION ALL
4. **Scalability**: Adding scope = new tables, new migrations, new code

### Why Composite Primary Key?

Deal IDs could theoretically collide across scopes (both systems start at ID 1). Composite key ensures uniqueness:

```sql
-- These are different records
(deal_id=100, scope='comercial')
(deal_id=100, scope='expansao')
```

## Trade-offs

| Trade-off | Mitigation |
|-----------|------------|
| Must filter by scope in queries | Views provide filtered access |
| Larger indexes | Scope column is small (VARCHAR 50) |
| Potential scope mixing errors | Application-level validation |

## Consequences

### Positive
- Single source of truth for deals across organization
- Easy comparative analytics
- Simpler ETL codebase
- Flexible for adding new scopes (future: franchises)

### Negative
- Queries must include scope filter or accept all scopes
- Larger single table (but still manageable at current scale)

## Future Considerations

If scope count grows significantly (10+ scopes), consider:
- Table partitioning by scope
- Row-level security for access control
- Materialized views per scope for performance

## References

- [Multi-tenant Database Design Patterns](https://docs.microsoft.com/azure/architecture/patterns/multi-tenancy)
- [PostgreSQL Table Partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html)
