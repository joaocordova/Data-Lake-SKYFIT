# ADR-001: PostgreSQL as Data Warehouse

## Status
**Accepted** - January 2026

## Context

We need to choose a data warehouse solution for the SkyFit Data Lake. The platform will store ~250k records initially, growing to potentially 1M+ records over 2 years. The data will be consumed by:
- Power BI dashboards (DirectQuery)
- Python ML pipelines (batch reads)
- Ad-hoc SQL queries (analysts)

### Options Considered

| Option | Monthly Cost | Complexity | Scalability |
|--------|-------------|------------|-------------|
| **PostgreSQL (Azure Flexible)** | ~$50 | Low | Medium |
| Snowflake | ~$300+ | Medium | High |
| Azure Synapse | ~$200+ | High | High |
| Databricks SQL | ~$400+ | Medium | High |

## Decision

Use **PostgreSQL 17 on Azure Flexible Server** as the data warehouse.

## Rationale

### Why PostgreSQL?

1. **Right-sized for current scale**: 250k rows is well within PostgreSQL's sweet spot. Enterprise DWs are overkill.

2. **JSONB support**: Critical for our staging layer. PostgreSQL's JSONB is industry-leading:
   ```sql
   -- Query nested JSON efficiently
   SELECT payload->>'title', payload->'person'->>'name'
   FROM stg_pipedrive.deals_raw
   WHERE payload->>'status' = 'won';
   ```

3. **Cost efficiency**: 6x cheaper than alternatives for our current scale.

4. **Operational simplicity**: Team already knows PostgreSQL. No new skills required.

5. **Migration path**: Easy to export to Snowflake/Databricks when needed (standard SQL, pg_dump).

### Trade-offs Accepted

| Trade-off | Mitigation |
|-----------|------------|
| Limited horizontal scaling | Sufficient for 10x growth; migrate when needed |
| No native time-travel | Implemented via run_id versioning in Bronze |
| No automatic query optimization | Manual indexing; acceptable at current scale |
| Single-region | Azure Flexible Server has HA options if needed |

## Consequences

### Positive
- 80% cost reduction vs enterprise alternatives
- Faster development (familiar technology)
- JSONB enables schema-flexible staging layer
- Easy local development (same DB engine)

### Negative
- Manual capacity planning required
- Will need migration when scale exceeds PostgreSQL limits
- No built-in data sharing (unlike Snowflake)

## Migration Trigger

Consider migration to Snowflake/Databricks when ANY of:
- Total records > 10M
- Query latency > 30s for typical dashboards  
- Need for concurrent warehouse scaling
- Cross-organization data sharing required

## References

- [PostgreSQL 17 Release Notes](https://www.postgresql.org/docs/17/release-17.html)
- [Azure Flexible Server Pricing](https://azure.microsoft.com/pricing/details/postgresql/flexible-server/)
- [When to use PostgreSQL vs Snowflake](https://www.datacamp.com/blog/postgresql-vs-snowflake)
