# SkyFit Data Lake

**Modern Data Platform for Fitness Industry Analytics**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![PostgreSQL 17](https://img.shields.io/badge/PostgreSQL-17-blue.svg)](https://www.postgresql.org/)
[![Azure ADLS Gen2](https://img.shields.io/badge/Azure-ADLS%20Gen2-0078D4.svg)](https://azure.microsoft.com/)

---

##  Overview

**SkyFit Data Lake** is a production-grade data platform that integrates multiple SaaS data sources into a unified analytical layer. Built following the **Medallion Architecture** pattern (Bronze → Silver → Gold), it provides a scalable foundation for business intelligence and machine learning workloads.

### Data Sources & Volumes

| Source | Entities | Volume | Status |
|--------|----------|--------|--------|
| **EVO (W12)** | Members, Sales, Entries, Prospects | ~130M records | ✅ Production |
| **Pipedrive** | Deals, Activities, Persons | ~170K records | ✅ Production |
| **Zendesk** | Tickets | ~12K records | ✅ Production |

### EVO Integration Metrics (Main Volume)

| Entity | Records | Bronze Size | STG Size | Processing Time |
|--------|---------|-------------|----------|-----------------|
| **Entries** | ~110M | ~45 GB | ~80 GB | ~8h extraction |
| **Sales** | ~12.8M | ~8 GB | ~20 GB | ~6h full load |
| **Members** | ~2.4M + 10M memberships | ~3 GB | ~15 GB | ~1.5h full load |
| **Prospects** | ~612K | ~500 MB | ~2 GB | ~30min |

---

##  Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                   │
├─────────────┬─────────────┬─────────────┬───────────────────────────────────┤
│  Pipedrive  │   Zendesk   │   EVO (W12) │         Future Sources            │
│    (CRM)    │  (Support)  │    (Gym)    │                                   │
└──────┬──────┴──────┬──────┴──────┬──────┴───────────────────────────────────┘
       │             │             │
       ▼             ▼             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER (Raw)                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Azure Data Lake Storage Gen2                                       │    │
│  │  Format: JSONL.GZ compressed                                        │    │
│  │  Partitioning: entity/ingestion_date/run_id                         │    │
│  │  Size: ~60 GB | Retention: Immutable, versioned                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER (STG)                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  PostgreSQL Flexible Server - stg_* schemas                         │    │
│  │  Format: JSONB raw_data with full lineage                           │    │
│  │  Size: ~120 GB | Purpose: Deduplication, validation                 │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER (CORE)                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  PostgreSQL Flexible Server - core schema                           │    │
│  │  Format: Normalized tables with proper data types                   │    │
│  │  Size: ~30 GB | Purpose: Analytics-ready, BI consumption            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CONSUMPTION                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Power BI   │  │   Python     │  │    SQL       │  │   REST API   │     │
│  │  Dashboards  │  │   ML/AI      │  │  Analytics   │  │   (Future)   │     │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Layer | Technology | Justification |
|-------|------------|---------------|
| **Extraction** | Python + multiprocessing | Parallel API calls, rate limiting control |
| **Raw Storage** | Azure ADLS Gen2 | Cost-effective, hierarchical namespace, native compression |
| **Processing** | Python + psycopg2 COPY | Bulk loading 50-100x faster than INSERT |
| **Warehouse** | PostgreSQL 17 Flexible | JSONB support, cost-efficient (~$85/month for 256GB) |
| **Orchestration** | Windows Task Scheduler | Simple, reliable for current scale |

---

##  Performance Optimizations

### Scripts v2 - Key Improvements

| Optimization | Description | Performance Gain |
|--------------|-------------|------------------|
| **ThreadedConnectionPool** | Reuses PostgreSQL connections across workers | 20-30% faster |
| **Batch Processing** | Groups 5-10 files per COPY operation | 2-3x less overhead |
| **Full-Refresh Mode** | TRUNCATE + INSERT without indexes | 5-10x faster |
| **Streaming Decompress** | Process files without loading entirely in memory | 50% less RAM |
| **Exponential Backoff Retry** | Survives Azure PostgreSQL failovers | 100% reliability |

### Benchmark Results

| Operation | v1 Time | v2 Time | Improvement |
|-----------|---------|---------|-------------|
| Sales Load (12.8M) | ~6h | ~2h | **3x** |
| Sales Transform | ~2h | ~30min | **4x** |
| Members Load (2.4M) | ~3h | ~1h | **3x** |
| Members Transform | ~4.6h | ~45min | **6x** |
| Entries Load (~110M) | ~15h | ~5h | **3x** |

### Processing Rates Achieved

| Operation | Records/Second | Method |
|-----------|----------------|--------|
| API Extraction | 140-220/s | Parallel workers with rate limiting |
| Bronze → STG Load | 500-600/s | COPY with temp tables |
| STG → CORE Transform | 4,000-7,000/s | Bulk INSERT with disabled indexes |

---

##  Cost Analysis & Trade-offs

### Infrastructure Costs (Monthly)

| Resource | Configuration | Cost |
|----------|---------------|------|
| Azure Data Lake Gen2 | ~60 GB, Cool tier | ~$5 |
| Azure PostgreSQL Flexible | B2ms, 256 GB | ~$85 |
| **Total** | | **~$90/month** |

### Trade-off: PostgreSQL vs Azure Synapse

| Criteria | PostgreSQL | Azure Synapse |
|----------|------------|---------------|
| Monthly Cost | ~$85 | ~$350-500 |
| Query Performance | Good for <500M rows | Better for billions |
| JSONB Support | Native, excellent | Limited |
| Maintenance | Low | Medium |
| BI Integration | DirectQuery OK | Optimized |

**Decision**: PostgreSQL chosen for current scale (~130M records). Synapse considered when volume exceeds 500M or query complexity increases significantly.

### Trade-off: COPY vs INSERT

| Method | 1M Records | Use Case |
|--------|------------|----------|
| Individual INSERT | ~8 hours | Never use for bulk |
| Batch INSERT (1000) | ~45 min | Small incremental |
| COPY with temp table | ~5 min | **Always for bulk** |

**Decision**: All loaders use COPY command with temp tables for 50-100x performance improvement.

---

## 📁 Project Structure

```
skyfit-datalake/
├── config/
│   └── .env                          # Environment variables
├── sql/
│   ├── evo_schemas.sql               # DDL for all EVO tables
│   ├── add_missing_columns.sql       # Schema migrations
│   ├── optimize_storage.sql          # VACUUM/REINDEX scripts
│   └── optimize_storage.py           # Storage analysis tool
├── evo_prospects/
│   └── src/
│       ├── loaders/
│       │   └── load_evo_prospects_stg_fast.py
│       └── transformers/
│           └── normalize_evo_prospects.py
├── evo_sales/
│   └── src/
│       ├── extractors/
│       │   └── evo_sales_bronze_parallel.py      # API → Bronze
│       ├── loaders/
│       │   ├── load_evo_sales_stg_fast.py        # v1
│       │   └── load_evo_sales_stg_fast_v2.py     # v2 optimized
│       └── transformers/
│           ├── normalize_evo_sales.py             # v1
│           └── normalize_evo_sales_v2.py          # v2 with full-refresh
├── evo_members/
│   └── src/
│       ├── extractors/
│       │   └── evo_members_bronze_parallel.py
│       ├── loaders/
│       │   ├── load_evo_members_stg_fast.py
│       │   └── load_evo_members_stg_fast_v2.py
│       └── transformers/
│           ├── normalize_evo_members.py
│           └── normalize_evo_members_v2.py
├── evo_entries/
│   └── src/
│       ├── extractors/
│       │   └── evo_entries_bronze_parallel.py
│       ├── loaders/
│       │   ├── load_evo_entries_stg_fast.py
│       │   └── load_evo_entries_stg_fast_v2.py
│       ├── transformers/
│       │   ├── normalize_evo_entries.py
│       │   └── normalize_evo_entries_v2.py
│       └── utils/
│           ├── analyze_extraction.py              # Gap analysis
│           └── diagnose_files.py                  # File structure debug
└── docs/
    ├── CUSTOS_E_ARQUITETURA.md
    ├── EVO_DATA_MODEL.md
    └── PERFORMANCE_TUNING.md
```

---

##  EVO Data Model

### CORE Layer Tables

#### core.evo_members
| Column | Type | Description |
|--------|------|-------------|
| member_id | BIGINT PK | Unique member ID |
| branch_id | BIGINT | Gym branch |
| first_name, last_name | TEXT | Name |
| document | TEXT | CPF |
| email, cellphone | TEXT | Contact |
| status | TEXT | Active/Inactive |
| membership_status | TEXT | Current membership status |
| gympass_id | TEXT | Gympass integration |
| code_totalpass | TEXT | Totalpass integration |
| register_date | TIMESTAMPTZ | Registration date |
| last_access_date | TIMESTAMPTZ | Last gym entry |

#### core.evo_member_memberships
| Column | Type | Description |
|--------|------|-------------|
| member_membership_id | BIGINT PK | Contract ID |
| member_id | BIGINT FK | Member reference |
| membership_id | BIGINT | Plan type ID |
| membership_name | TEXT | Plan name |
| start_date, end_date | DATE | Contract period |
| membership_status | TEXT | active/expired/canceled |
| sale_id | BIGINT | Related sale |

#### core.evo_sales
| Column | Type | Description |
|--------|------|-------------|
| sale_id | BIGINT PK | Sale ID |
| branch_id | BIGINT | Branch |
| member_id | BIGINT | Member |
| sale_date | TIMESTAMPTZ | Sale timestamp |
| total_value | DECIMAL(15,2) | Total amount |
| payment_status | TEXT | Payment status |

#### core.evo_entries
| Column | Type | Description |
|--------|------|-------------|
| entry_id | TEXT PK | MD5 hash (deterministic) |
| member_id | BIGINT | Member |
| branch_id | BIGINT | Branch |
| entry_date | TIMESTAMPTZ | Entry timestamp |
| register_date | TIMESTAMPTZ | Record creation |

> Full schema: [sql/evo_schemas.sql](sql/evo_schemas.sql)

---

##  Quick Start

### 1. Prerequisites

```bash
pip install psycopg2-binary azure-storage-file-datalake python-dotenv requests
```

### 2. Configuration

```bash
# config/.env
AZURE_STORAGE_ACCOUNT=your_storage_account
AZURE_STORAGE_KEY=your_key
ADLS_CONTAINER=datalake

PG_HOST=your_server.postgres.database.azure.com
PG_PORT=5432
PG_DATABASE=postgres
PG_USER=your_user
PG_PASSWORD=your_password
PG_SSLMODE=require

EVO_API_USERNAME=your_evo_user
EVO_API_PASSWORD=your_evo_password
```

### 3. Create Schema

```powershell
psql -h $env:PG_HOST -U $env:PG_USER -d $env:PG_DATABASE -f "sql/evo_schemas.sql"
```

### 4. Run Pipeline

```powershell
# Extract (API → Bronze)
cd C:\skyfit-datalake\evo_members
python src/extractors/evo_members_bronze_parallel.py --workers 8

# Load (Bronze → STG) - v2 optimized
python src/loaders/load_evo_members_stg_fast_v2.py --workers 8 --batch-size 10 --all-runs

# Transform (STG → CORE) - full refresh mode
python src/transformers/normalize_evo_members_v2.py --full-refresh
```

---

##  Operations

### Storage Optimization

PostgreSQL index bloat can consume 80%+ of storage after repeated UPSERTs.

```powershell
# Analyze current state
python sql/optimize_storage.py --analyze

# Run optimization (maintenance window)
python sql/optimize_storage.py --vacuum --reindex
```

Expected savings: ~90 GB recovered from bloated indexes.

### Retry Mechanism

Scripts v2 implement exponential backoff for Azure PostgreSQL failovers:

```
Attempt 1: wait 30s
Attempt 2: wait 60s
Attempt 3: wait 120s
Attempt 4: wait 240s
```

Handled errors:
- `read-only transaction` (failover in progress)
- `connection already closed`
- `server closed the connection`

### Gap Analysis for Entries

```powershell
cd C:\skyfit-datalake\evo_entries
python src/utils/diagnose_files.py
```

Output shows extracted periods and command to continue interrupted extraction.

---

##  Monitoring

### Key Queries

```sql
-- Table sizes
SELECT 
    schemaname || '.' || tablename AS table,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS total
FROM pg_tables 
WHERE schemaname IN ('stg_evo', 'core')
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;

-- Record counts
SELECT 'members' AS entity, COUNT(*) FROM core.evo_members
UNION ALL SELECT 'memberships', COUNT(*) FROM core.evo_member_memberships
UNION ALL SELECT 'sales', COUNT(*) FROM core.evo_sales
UNION ALL SELECT 'entries', COUNT(*) FROM core.evo_entries;

-- Index bloat check
SELECT 
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size,
    idx_scan AS reads
FROM pg_stat_user_indexes
WHERE schemaname = 'stg_evo'
ORDER BY pg_relation_size(indexrelid) DESC;
```

---

##  Troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| `column does not exist` | Schema outdated | Run `sql/add_missing_columns.sql` |
| `read-only transaction` | Azure failover | Wait 30-60s, retry automatically (v2) |
| `storage full` | Index bloat | Run `optimize_storage.py --vacuum --reindex` |
| `connection refused` | Network/firewall | Check Azure PostgreSQL firewall rules |

---

##  Changelog

### v9 (2026-01-14)
- ✅ Added `code_totalpass` to Members
- ✅ Added `user_id_gurupass` to Members
- ✅ Scripts v2 with ThreadedConnectionPool
- ✅ Full-refresh mode for transforms (5-10x faster)
- ✅ Storage optimization tools
- ✅ Entries gap analysis utilities

### v7 (2026-01-12)
- ✅ Exponential backoff retry for Azure failovers
- ✅ Multi run_id support (--all-runs flag)

### v5 (2026-01-11)
- ✅ Non-destructive schema (CREATE IF NOT EXISTS)
- ✅ Complete Entries pipeline with hash-based IDs

---

## 👤 Author

**João V. Cordova** - Data Engineer - [GitHub](https://github.com/joaocordova)

---

Built with ❤️ for the fitness industry
