# Architecture Decisions & Cost Analysis

This document explains the architectural trade-offs and cost considerations for the SkyFit Data Lake.

---

## Architecture Decision Records (ADRs)

### ADR-001: PostgreSQL over Azure Synapse

**Context**: Need a warehouse solution for ~130M records with analytics workloads.

**Options Considered**:

| Option | Monthly Cost | Pros | Cons |
|--------|--------------|------|------|
| Azure Synapse Serverless | $350-500 | Scalable, native Azure | High cost, complex setup |
| Azure Synapse Dedicated | $800+ | Best performance | Very expensive |
| PostgreSQL Flexible | $85 | Cost-effective, familiar | Limited at scale |
| Snowflake | $400+ | Modern, scalable | Vendor lock-in |

**Decision**: PostgreSQL Flexible Server (B2ms, 256GB)

**Rationale**:
- Current volume (~130M records) well within PostgreSQL capabilities
- 80% cost savings vs Synapse
- Native JSONB support for flexible staging
- Team familiarity with PostgreSQL
- Easy migration path to Synapse if needed later

**Consequences**:
- May need migration when volume exceeds 500M records
- Some analytical queries slower than Synapse
- Need to manage index bloat manually

---

### ADR-002: JSONB Staging Layer

**Context**: API responses have nested structures (memberships, contacts, items).

**Options Considered**:

| Option | Flexibility | Query Performance | Storage |
|--------|-------------|-------------------|---------|
| Normalized tables | Low | High | Low |
| JSONB single column | High | Medium | Medium |
| Parquet files | High | High | Low |

**Decision**: JSONB `raw_data` column in STG tables

**Rationale**:
- Absorbs API schema changes without DDL
- Preserves complete original data
- GIN indexes enable efficient JSONB queries
- Transformation to CORE extracts needed fields

**Consequences**:
- Larger storage footprint in STG layer
- Some queries require JSONB operators
- Need to maintain transformation logic

---

### ADR-003: COPY Command for Bulk Loading

**Context**: Initial loads of millions of records.

**Options Considered**:

| Method | 1M Records | Scalability |
|--------|------------|-------------|
| Individual INSERT | 8 hours | Poor |
| Batch INSERT | 45 min | Medium |
| COPY command | 5 min | Excellent |

**Decision**: COPY with temporary tables and UPSERT

**Rationale**:
- 50-100x faster than INSERT
- Native PostgreSQL feature
- Works with psycopg2 (no additional dependencies)

**Consequences**:
- Requires temp table creation per batch
- Tab-separated format needs escaping
- All-or-nothing per batch (no partial failures)

---

### ADR-004: Deterministic Entry ID Generation

**Context**: EVO Entries API lacks a primary key.

**Options Considered**:

| Option | Uniqueness | Idempotency | Complexity |
|--------|------------|-------------|------------|
| UUID | Yes | No | Low |
| Auto-increment | Yes | No | Low |
| MD5 hash of fields | Yes | Yes | Medium |
| Composite key | Yes | Yes | High |

**Decision**: MD5 hash of `member_id|branch_id|entry_date|register_date`

**Rationale**:
- Deterministic: same record always gets same ID
- Idempotent: re-processing doesn't create duplicates
- Single column simplifies UPSERT logic

**Consequences**:
- Hash collisions theoretically possible (extremely rare)
- ID not human-readable
- Must include all key fields in hash

---

### ADR-005: Connection Pooling

**Context**: High connection overhead in multi-threaded loaders.

**Options Considered**:

| Option | Overhead | Complexity | Thread-safe |
|--------|----------|------------|-------------|
| Connection per file | High | Low | Yes |
| Shared connection | Low | Medium | No |
| ThreadedConnectionPool | Low | Medium | Yes |
| PgBouncer | Lowest | High | Yes |

**Decision**: `psycopg2.pool.ThreadedConnectionPool`

**Rationale**:
- Built into psycopg2 (no external dependency)
- Thread-safe for parallel workers
- 20-30% performance improvement

**Consequences**:
- Pool size must be configured correctly
- Connections must be properly returned
- Not as efficient as PgBouncer for very high concurrency

---

### ADR-006: Exponential Backoff Retry

**Context**: Azure PostgreSQL failovers cause connection errors.

**Options Considered**:

| Option | Recovery Time | Complexity | Reliability |
|--------|---------------|------------|-------------|
| No retry | Manual intervention | Low | Low |
| Fixed delay retry | ~5 min | Low | Medium |
| Exponential backoff | ~2 min | Medium | High |
| Circuit breaker | Variable | High | Highest |

**Decision**: Exponential backoff with 5 retries (30s, 60s, 120s, 240s)

**Rationale**:
- Handles typical Azure failover duration (30-60s)
- Doesn't overwhelm server during recovery
- Simple to implement and understand

**Consequences**:
- Long jobs can recover automatically
- May wait unnecessarily if error is permanent
- Need to distinguish failover from other errors

---

## Cost Analysis

### Current Infrastructure

| Resource | Configuration | Monthly Cost |
|----------|---------------|--------------|
| Azure Data Lake Gen2 | ~60 GB, Cool tier | $5 |
| Azure PostgreSQL Flexible | B2ms (2 vCores), 256 GB | $85 |
| **Total** | | **$90** |

### Cost Breakdown

```
PostgreSQL Flexible Server (B2ms):
├── Compute (2 vCores): $50/month
├── Storage (256 GB): $29/month
├── Backup (7 days): $6/month
└── Network: ~$0 (same region)

Azure Data Lake Gen2:
├── Storage (60 GB, Cool): $1/month
├── Operations: $4/month
└── Egress: ~$0 (same region)
```

### Cost Comparison with Alternatives

| Solution | ~130M Records | ~500M Records | ~1B Records |
|----------|---------------|---------------|-------------|
| PostgreSQL | $90 | $150 | Migration needed |
| Azure Synapse Serverless | $350 | $500 | $800 |
| Azure Synapse Dedicated | $800 | $1,200 | $2,000 |
| Snowflake | $400 | $700 | $1,200 |
| Databricks | $500 | $800 | $1,500 |

**Savings**: ~$260-400/month vs cloud-native alternatives.

### When to Migrate to Synapse

Consider migration when:
- Total records exceed 500M
- Query response time exceeds 30s for typical analytics
- Need real-time dashboards on hot data
- ML workloads require distributed computing

---

## Data Volume Projections

### Current State

| Entity | Records | Monthly Growth | Year-End Projection |
|--------|---------|----------------|---------------------|
| Entries | 110M | +10M | 230M |
| Sales | 12.8M | +500K | 18.8M |
| Members | 2.4M | +100K | 3.6M |
| Memberships | 10M | +1M | 22M |

### Storage Projections

| Timeframe | Bronze | STG | CORE | Total |
|-----------|--------|-----|------|-------|
| Current | 60 GB | 120 GB | 30 GB | 210 GB |
| 6 months | 90 GB | 180 GB | 45 GB | 315 GB |
| 12 months | 120 GB | 240 GB | 60 GB | 420 GB |

**Recommendation**: PostgreSQL remains suitable for 12+ months at current growth rate.

---

## Performance vs Cost Trade-offs

### Compute Tier Selection

| Tier | vCores | RAM | Cost | Use Case |
|------|--------|-----|------|----------|
| B1ms | 1 | 2 GB | $25 | Dev/Test |
| B2ms | 2 | 4 GB | $50 | **Current Production** |
| GP_Gen5_2 | 2 | 10 GB | $120 | Heavy queries |
| GP_Gen5_4 | 4 | 20 GB | $240 | High concurrency |

**Current choice**: B2ms provides best cost/performance ratio for current workload.

### Storage Tier Selection

| Tier | IOPS | Cost/GB | Use Case |
|------|------|---------|----------|
| Standard | 120 | $0.115 | Archive |
| General Purpose | 500 | $0.115 | **Current** |
| Premium | 3000 | $0.140 | High I/O |

**Current choice**: General Purpose provides sufficient IOPS for batch workloads.

### Index Strategy Trade-off

| Strategy | Load Speed | Query Speed | Storage |
|----------|------------|-------------|---------|
| Minimal indexes | Fast | Slow | Low |
| Full indexes | Slow | Fast | High |
| Drop/Recreate | Fast (full-refresh) | Fast (after load) | Medium |

**Current approach**: Drop indexes during full-refresh, recreate after. Best of both worlds.

---

## Risk Analysis

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| PostgreSQL outgrows scale | Medium | High | Monitor metrics, plan Synapse migration |
| Azure failover data loss | Low | High | Retry mechanism, idempotent loads |
| Index bloat fills storage | High | Medium | Regular VACUUM/REINDEX |
| API rate limiting | Medium | Low | Parallel workers with backoff |

### Operational Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Schema changes break pipeline | Medium | Medium | JSONB staging absorbs changes |
| Long-running jobs fail | Medium | Medium | Retry mechanism, checkpoint logging |
| Storage costs spike | Low | Low | Monitor usage, archive old data |

---

## Recommendations

### Immediate (0-3 months)
- ✅ Complete Entries extraction
- ✅ Implement storage optimization
- ⏳ Set up monitoring dashboards
- ⏳ Document runbooks

### Short-term (3-6 months)
- Add data quality checks (Great Expectations)
- Implement incremental extraction for Entries
- Consider partitioning for Entries table

### Medium-term (6-12 months)
- Evaluate Synapse for heavy analytics
- Add ML feature store
- Implement CDC for near-real-time updates

### Long-term (12+ months)
- Full migration to modern data stack
- Real-time streaming with Event Hubs
- Self-service analytics with semantic layer
