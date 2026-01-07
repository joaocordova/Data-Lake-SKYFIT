<p align="center">
  <img src="docs/assets/logo-placeholder.png" alt="SkyFit Data Lake" width="200"/>
</p>

<h1 align="center">SkyFit Data Lake</h1>

<p align="center">
  <strong>Production-Grade Data Platform for Fitness Industry Analytics & ML</strong>
</p>

<p align="center">
  <a href="#architecture">Architecture</a> •
  <a href="#key-decisions">Key Decisions</a> •
  <a href="#data-model">Data Model</a> •
  <a href="#getting-started">Getting Started</a> •
  <a href="#operations">Operations</a> •
  <a href="#roadmap">Roadmap</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="Python 3.10+"/>
  <img src="https://img.shields.io/badge/PostgreSQL-17-336791.svg" alt="PostgreSQL 17"/>
  <img src="https://img.shields.io/badge/Azure-ADLS%20Gen2-0078D4.svg" alt="Azure ADLS Gen2"/>
  <img src="https://img.shields.io/badge/Architecture-Medallion-gold.svg" alt="Medallion Architecture"/>
  <img src="https://img.shields.io/badge/Pipeline-Idempotent-green.svg" alt="Idempotent Pipeline"/>
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License"/>
</p>

---

## 📋 Executive Summary

**SkyFit Data Lake** is an end-to-end data platform that consolidates data from multiple SaaS systems (CRM, Support, Gym Management) into a unified analytical layer. Built with production-grade patterns, it serves as the foundation for business intelligence dashboards and machine learning models (churn prediction, customer LTV).

### Business Impact

| Metric | Before | After |
|--------|--------|-------|
| **Data Freshness** | Manual exports, days old | Automated daily refresh |
| **Cross-system Analysis** | Impossible (data silos) | Unified view across CRM + Support |
| **ML Readiness** | None | Production-ready feature store |
| **Time to Insight** | Hours (manual joins) | Minutes (pre-joined tables) |

### Current Scale

| Source | Records | Daily Delta | Status |
|--------|---------|-------------|--------|
| Pipedrive CRM (2 scopes) | ~227k records | ~200-500 | ✅ Production |
| Zendesk Support | ~25k records | ~50-100 | ✅ Production |
| EVO Gym Management | ~600k prospects | TBD | 🔄 Phase 2 |

---

## 🏗️ Architecture

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES (SaaS APIs)                            │
├───────────────────┬───────────────────┬───────────────────┬─────────────────────┤
│     Pipedrive     │      Zendesk      │        EVO        │   Future Sources    │
│   (CRM - 2 BUs)   │    (Support)      │  (Gym Mgmt)       │                     │
│  REST API + OAuth │  REST API + Token │  REST API + Token │                     │
└─────────┬─────────┴─────────┬─────────┴─────────┬─────────┴─────────────────────┘
          │                   │                   │
          ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        🥉 BRONZE LAYER (Raw/Immutable)                           │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  Azure Data Lake Storage Gen2 (Cool Tier)                                 │  │
│  │                                                                           │  │
│  │  Format: .jsonl.gz (compressed JSON Lines)                                │  │
│  │  Partitioning: Hive-style                                                 │  │
│  │    bronze/{source}/scope={scope}/entity={entity}/                         │  │
│  │           ingestion_date={YYYY-MM-DD}/run_id={timestamp}/                 │  │
│  │                                                                           │  │
│  │  Key Properties:                                                          │  │
│  │    • Immutable (append-only, never modified)                              │  │
│  │    • Versioned by run_id (full audit trail)                               │  │
│  │    • Compressed (~10:1 ratio)                                             │  │
│  │    • Schema-agnostic (raw JSON preserved)                                 │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
          │
          │  Watermark-based incremental extraction
          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        🥈 SILVER LAYER (Staging/Validated)                       │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  PostgreSQL 17 - stg_pipedrive.* / stg_zendesk.*                          │  │
│  │                                                                           │  │
│  │  Format: JSONB (schema-flexible)                                          │  │
│  │  ┌─────────────────────────────────────────────────────────────────────┐  │  │
│  │  │ deals_raw                                                           │  │  │
│  │  │ ├── payload (JSONB)         -- Original API response                │  │  │
│  │  │ ├── scope (VARCHAR)         -- Business unit identifier             │  │  │
│  │  │ ├── source_blob_path        -- Full lineage to Bronze               │  │  │
│  │  │ ├── source_line_no          -- Exact line in source file            │  │  │
│  │  │ ├── run_id                  -- Extraction batch identifier          │  │  │
│  │  │ ├── ingestion_date          -- Partition key                        │  │  │
│  │  │ └── loaded_at               -- ETL timestamp                        │  │  │
│  │  └─────────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                           │  │
│  │  Key Properties:                                                          │  │
│  │    • Full data lineage (trace any record to source)                       │  │
│  │    • Deduplication via UPSERT                                             │  │
│  │    • Schema evolution tolerant (JSONB absorbs changes)                    │  │
│  │    • Queryable for debugging                                              │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
          │
          │  JSONB extraction + type casting + deduplication
          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         🥇 GOLD LAYER (Core/Analytics)                           │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  PostgreSQL 17 - core.*                                                   │  │
│  │                                                                           │  │
│  │  Format: Normalized star schema with proper data types                    │  │
│  │                                                                           │  │
│  │  Pipedrive Tables:           Zendesk Tables:                              │  │
│  │  ├── pd_pipelines            ├── zd_organizations                         │  │
│  │  ├── pd_stages               ├── zd_users                                 │  │
│  │  ├── pd_users                ├── zd_groups                                │  │
│  │  ├── pd_organizations        ├── zd_ticket_fields                         │  │
│  │  ├── pd_persons              ├── zd_ticket_forms                          │  │
│  │  ├── pd_deals ←─ FACT        ├── zd_tickets ←─ FACT                       │  │
│  │  └── pd_activities ←─ FACT   ├── zd_ticket_tags                           │  │
│  │                              └── zd_ticket_custom_fields                  │  │
│  │                                                                           │  │
│  │  Key Properties:                                                          │  │
│  │    • Strongly typed columns                                               │  │
│  │    • Indexed for analytical queries                                       │  │
│  │    • Multi-tenant support (scope column)                                  │  │
│  │    • ML-ready (consistent schemas)                                        │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              📊 CONSUMPTION LAYER                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐ │
│  │   Power BI   │  │   Jupyter    │  │   Python     │  │   ML Pipeline        │ │
│  │  Dashboards  │  │  Notebooks   │  │   Scripts    │  │  (Churn/LTV)         │ │
│  │              │  │              │  │              │  │                      │ │
│  │ DirectQuery  │  │  Ad-hoc      │  │  Scheduled   │  │  Feature Store       │ │
│  │ to core.*    │  │  Analysis    │  │  Reports     │  │  Training Data       │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Component | Technology | Rationale |
|-----------|------------|-----------|
| **Raw Storage** | Azure ADLS Gen2 (Cool) | Cost-effective for write-once data, hierarchical namespace for partitioning |
| **Warehouse** | PostgreSQL 17 | JSONB for schema flexibility, mature ecosystem, Azure Flexible Server cost-efficiency |
| **Language** | Python 3.10+ | Rich ecosystem for API integrations, async support |
| **Orchestration** | Windows Task Scheduler | Simple, reliable for single-node; migration path to Airflow/ADF |
| **BI** | Power BI | Enterprise standard, DirectQuery to PostgreSQL |

---

## 🎯 Key Decisions (ADRs)

### ADR-001: PostgreSQL over Dedicated Data Warehouse

**Context**: Choose between PostgreSQL, Snowflake, Databricks, or BigQuery.

**Decision**: PostgreSQL 17 on Azure Flexible Server.

**Rationale**:
| Factor | PostgreSQL | Snowflake/Databricks |
|--------|------------|---------------------|
| **Current Volume** | ~250k rows | Overkill |
| **Cost** | ~$50/month | ~$300+/month |
| **JSONB Support** | Native, excellent | Limited |
| **Operational Complexity** | Low | Medium-High |
| **Migration Path** | Easy export to any DW | N/A |

**Trade-off**: Less horizontal scalability, but sufficient for 10x growth. When volumes exceed 10M+ rows or require complex transformations, migrate to Snowflake/Databricks.

---

### ADR-002: JSONB Staging Layer

**Context**: How to handle schema evolution from SaaS APIs?

**Decision**: Store raw payloads as JSONB in Silver layer before normalizing.

**Rationale**:
- APIs change schemas without notice (new fields, renamed fields)
- JSONB absorbs changes without pipeline breaks
- Enables retrospective extraction of new fields
- Full payload preserved for debugging

**Trade-off**: Slightly larger storage, extraction queries more complex. Worth it for resilience.

---

### ADR-003: Multi-Scope Single Table vs Separate Tables

**Context**: Pipedrive has 2 business units (comercial, expansao). Store in one table or separate?

**Decision**: Single table with `scope` column as part of composite primary key.

**Rationale**:
```sql
-- Single table approach (chosen)
PRIMARY KEY (deal_id, scope)

-- Enables cross-scope queries easily
SELECT scope, COUNT(*), AVG(value) FROM core.pd_deals GROUP BY scope;

-- Views for scope isolation when needed
CREATE VIEW core.vw_pd_deals_comercial AS 
SELECT * FROM core.pd_deals WHERE scope = 'comercial';
```

**Trade-off**: Requires filtering in queries. Mitigated by views and proper indexing.

---

### ADR-004: Watermark-based Incremental Extraction

**Context**: How to efficiently extract only new/changed records?

**Decision**: Maintain watermarks (last `update_time`) per entity/scope in ADLS.

```
_meta/pipedrive/watermarks/scope=comercial/entity=deals.json
{"last_update_time": "2026-01-07T19:38:01Z"}
```

**Rationale**:
- APIs support filtering by `update_time`
- Reduces API calls by 95%+ after initial load
- Idempotent: can re-run without duplicates
- Recoverable: delete watermark to force full reload

---

### ADR-005: Run-based Versioning

**Context**: How to track extraction batches and enable replay?

**Decision**: Every extraction generates a unique `run_id` (timestamp-based).

```
bronze/pipedrive/scope=comercial/entity=deals/
  ingestion_date=2026-01-07/
    run_id=20260107T193801Z/
      part-00001.jsonl.gz
```

**Rationale**:
- Full audit trail: which records came from which extraction
- Enables replay: re-process specific runs if issues found
- Supports SLA tracking: when was data extracted?
- Partition pruning for efficient queries

---

## 📊 Data Model

### Entity Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              PIPEDRIVE (CRM)                                     │
│                                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                     │
│  │ pd_pipelines │────<│  pd_stages   │     │   pd_users   │                     │
│  │──────────────│     │──────────────│     │──────────────│                     │
│  │ pipeline_id  │     │ stage_id     │     │ user_id      │                     │
│  │ name         │     │ pipeline_id  │     │ name         │                     │
│  │ scope        │     │ name         │     │ email        │                     │
│  └──────────────┘     │ order_nr     │     │ scope        │                     │
│                       │ scope        │     └──────┬───────┘                     │
│                       └──────┬───────┘            │                             │
│                              │                    │                             │
│  ┌──────────────┐           │                    │        ┌──────────────┐     │
│  │pd_organizat° │           │                    │        │ pd_activities│     │
│  │──────────────│           │                    │        │──────────────│     │
│  │ org_id       │           │                    │        │ activity_id  │     │
│  │ name         │◄──────────┼────────────────────┼───────►│ deal_id      │     │
│  │ scope        │           │                    │        │ person_id    │     │
│  └──────┬───────┘           │                    │        │ user_id      │     │
│         │                   │                    │        │ type         │     │
│         │                   ▼                    ▼        │ done         │     │
│         │           ┌─────────────────────────────────┐   │ scope        │     │
│         │           │          pd_deals (FACT)        │   └──────────────┘     │
│         │           │─────────────────────────────────│                        │
│         └──────────►│ deal_id (PK)                    │◄───────────────────────┘│
│                     │ scope (PK)                      │                        │
│  ┌──────────────┐   │ person_id (FK)                  │                        │
│  │  pd_persons  │   │ org_id (FK)                     │                        │
│  │──────────────│   │ user_id (FK) -- owner           │                        │
│  │ person_id    │──►│ pipeline_id (FK)                │                        │
│  │ name         │   │ stage_id (FK)                   │                        │
│  │ email        │   │ title, value, currency          │                        │
│  │ phone        │   │ status (open/won/lost)          │                        │
│  │ org_id       │   │ won_time, lost_time, close_time │                        │
│  │ scope        │   │ expected_close_date             │                        │
│  └──────────────┘   │ add_time, update_time           │                        │
│                     │ _loaded_at, _updated_at         │                        │
│                     └─────────────────────────────────┘                        │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                              ZENDESK (Support)                                   │
│                                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                     │
│  │zd_organizat° │     │  zd_groups   │     │zd_ticket_flds│                     │
│  │──────────────│     │──────────────│     │──────────────│                     │
│  │ org_id       │     │ group_id     │     │ field_id     │                     │
│  │ name         │     │ name         │     │ title        │                     │
│  │ domain_names │     │ description  │     │ type         │                     │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘                     │
│         │                    │                    │                             │
│         │                    │                    │                             │
│         │           ┌────────┴────────┐          │                             │
│         │           │    zd_users     │          │                             │
│         │           │─────────────────│          │                             │
│         └──────────►│ user_id         │          │                             │
│                     │ name, email     │          │                             │
│                     │ role            │          │                             │
│                     │ organization_id │          │                             │
│                     └────────┬────────┘          │                             │
│                              │                   │                             │
│                              ▼                   ▼                             │
│                     ┌─────────────────────────────────┐                        │
│                     │       zd_tickets (FACT)         │                        │
│                     │─────────────────────────────────│                        │
│                     │ ticket_id (PK)                  │                        │
│                     │ requester_id (FK → users)       │                        │
│                     │ assignee_id (FK → users)        │                        │
│                     │ group_id (FK)                   │                        │
│                     │ organization_id (FK)            │                        │
│                     │ subject, description            │                        │
│                     │ status, priority                │◄──┐                    │
│                     │ channel (via_channel)           │   │                    │
│                     │ created_at, updated_at          │   │                    │
│                     │ solved_at, first_reply_at       │   │                    │
│                     │ _loaded_at, _updated_at         │   │                    │
│                     └──────────────┬──────────────────┘   │                    │
│                                    │                      │                    │
│                     ┌──────────────┴───────┐    ┌────────┴───────┐            │
│                     │   zd_ticket_tags     │    │zd_ticket_custom│            │
│                     │──────────────────────│    │────────────────│            │
│                     │ ticket_id (PK,FK)    │    │ ticket_id (FK) │            │
│                     │ tag (PK)             │    │ field_id (FK)  │            │
│                     └──────────────────────┘    │ value          │            │
│                                                 └────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Record Counts (Production)

| Layer | Table | Records | Notes |
|-------|-------|---------|-------|
| **Gold** | `core.pd_deals` | 31,008 | 30.6k comercial + 352 expansao |
| **Gold** | `core.pd_persons` | 44,294 | Contacts |
| **Gold** | `core.pd_activities` | 140,753 | Calls, meetings, tasks |
| **Gold** | `core.pd_organizations` | 10,695 | Companies |
| **Gold** | `core.zd_tickets` | 11,799 | Support tickets |
| **Gold** | `core.zd_users` | 1,374 | Agents + customers |
| **Gold** | `core.zd_ticket_tags` | 10,222 | Normalized tags |
| **Total** | - | **~250k** | Growing ~500/day |

---

## 🚀 Getting Started

### Prerequisites

```bash
# Required
- Python 3.10+
- PostgreSQL 17 (Azure Flexible Server or local)
- Azure Storage Account with ADLS Gen2 enabled

# API Credentials
- Pipedrive API tokens (one per scope)
- Zendesk API token + admin email
```

### Installation

```powershell
# 1. Clone repository
git clone https://github.com/joaocordova/Data-Lake-SKYFIT.git
cd Data-Lake-SKYFIT

# 2. Create virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
copy config\.env.example config\.env
# Edit .env with your credentials

# 5. Initialize database schemas
psql -h $PG_HOST -U $PG_USER -d $PG_DATABASE -f sql/schemas/00_create_schemas.sql
psql -h $PG_HOST -U $PG_USER -d $PG_DATABASE -f sql/schemas/01_stg_pipedrive.sql
psql -h $PG_HOST -U $PG_USER -d $PG_DATABASE -f sql/schemas/02_stg_zendesk.sql
psql -h $PG_HOST -U $PG_USER -d $PG_DATABASE -f sql/schemas/04_core_tables.sql

# 6. Run initial extraction (first time takes longer)
python src/extractors/pipedrive_bronze.py
python src/extractors/zendesk_bronze.py

# 7. Load to staging
python src/loaders/load_pipedrive_stg.py
python src/loaders/load_zendesk_stg.py

# 8. Transform to core
python src/transformers/normalize_pipedrive.py
python src/transformers/normalize_zendesk.py

# 9. Validate
.\scripts\health_check.ps1
```

### Quick Start (Daily Pipeline)

```powershell
# Run full pipeline: Bronze → Silver → Gold → Health Check
.\scripts\daily_pipeline.ps1

# Or schedule automatically (runs daily at 6:00 AM)
.\scripts\setup_task_scheduler.ps1 -TaskTime "06:00"
```

---

## ⚙️ Operations

### Pipeline Commands

| Action | Command |
|--------|---------|
| **Full pipeline** | `.\scripts\daily_pipeline.ps1` |
| **Bronze only** | `python src/extractors/pipedrive_bronze.py` |
| **Silver only** | `python src/loaders/load_pipedrive_stg.py` |
| **Gold only** | `python src/transformers/normalize_pipedrive.py` |
| **Health check** | `.\scripts\health_check.ps1` |
| **Skip layers** | `.\scripts\daily_pipeline.ps1 -SkipBronze -SkipSilver` |

### Task Scheduler

| Action | Command |
|--------|---------|
| **Setup** | `.\scripts\setup_task_scheduler.ps1 -TaskTime "06:00"` |
| **Run now** | `schtasks.exe /Run /TN "SkyFit - Daily Pipeline"` |
| **Check status** | `schtasks.exe /Query /TN "SkyFit - Daily Pipeline" /V` |
| **Disable** | `schtasks.exe /Change /TN "SkyFit - Daily Pipeline" /DISABLE` |
| **Remove** | `schtasks.exe /Delete /TN "SkyFit - Daily Pipeline" /F` |

### Force Full Reload

```powershell
# Delete watermarks to force full extraction (example: expansao scope)
python -c "
from config.settings import azure_storage
from azure.storage.blob import ContainerClient

container = ContainerClient(
    account_url=f'https://{azure_storage.ACCOUNT}.blob.core.windows.net',
    container_name=azure_storage.CONTAINER,
    credential=azure_storage.KEY
)

for blob in container.list_blobs(name_starts_with='_meta/pipedrive/watermarks/scope=expansao/'):
    container.get_blob_client(blob.name).delete_blob()
    print(f'Deleted: {blob.name}')
"

# Then run extraction
python src/extractors/pipedrive_bronze.py --scopes expansao
```

### Monitoring

```powershell
# View latest logs
Get-Content (Get-ChildItem "logs\daily_pipeline_*.log" | 
  Sort-Object LastWriteTime -Descending | 
  Select-Object -First 1).FullName -Tail 100

# Check data freshness
.\scripts\health_check.ps1
```

---

## 📁 Project Structure

```
skyfit-datalake/
│
├── 📂 config/                      # Configuration
│   ├── .env.example                # Environment template
│   └── settings.py                 # Centralized settings loader
│
├── 📂 docs/                        # Documentation
│   ├── 📂 architecture/            # Architecture diagrams & ADRs
│   │   ├── README.md               # Architecture overview
│   │   └── decisions/              # Architecture Decision Records
│   ├── 📂 data-catalog/            # Data dictionary
│   └── OPERATIONS_GUIDE.md         # Runbook
│
├── 📂 scripts/                     # Automation
│   ├── daily_pipeline.ps1          # Main orchestrator
│   ├── health_check.ps1            # Data validation
│   ├── setup_task_scheduler.ps1    # Windows scheduler setup
│   └── diagnose.py                 # Troubleshooting utility
│
├── 📂 sql/                         # Database artifacts
│   ├── 📂 schemas/                 # DDL scripts
│   │   ├── 00_create_schemas.sql   # Schema creation
│   │   ├── 01_stg_pipedrive.sql    # Pipedrive staging tables
│   │   ├── 02_stg_zendesk.sql      # Zendesk staging tables
│   │   ├── 04_core_tables.sql      # Core analytical tables
│   │   └── 05_scope_views.sql      # Convenience views
│   └── 📂 validations/             # Data quality queries
│
├── 📂 src/                         # Source code
│   ├── 📂 common/                  # Shared utilities
│   │   ├── lake.py                 # ADLS client wrapper
│   │   ├── db.py                   # PostgreSQL utilities
│   │   └── logging_config.py       # Structured logging
│   │
│   ├── 📂 extractors/              # Bronze layer (API → ADLS)
│   │   ├── pipedrive_bronze.py     # Pipedrive extractor
│   │   └── zendesk_bronze.py       # Zendesk extractor
│   │
│   ├── 📂 loaders/                 # Silver layer (ADLS → STG)
│   │   ├── load_pipedrive_stg.py   # Pipedrive loader
│   │   └── load_zendesk_stg.py     # Zendesk loader
│   │
│   └── 📂 transformers/            # Gold layer (STG → CORE)
│       ├── normalize_pipedrive.py  # Pipedrive transformer
│       └── normalize_zendesk.py    # Zendesk transformer
│
├── 📂 logs/                        # Pipeline execution logs
├── 📂 tests/                       # Test suite
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🗺️ Roadmap & Scaling Strategy

### Current State (v1.0)

```
✅ Pipedrive integration (2 scopes)
✅ Zendesk integration
✅ Medallion architecture (Bronze/Silver/Gold)
✅ Idempotent pipelines
✅ Windows Task Scheduler automation
✅ Health checks & monitoring
```

### Phase 2: EVO Integration (Q1 2026)

```
🔄 EVO Gym Management API integration
   - ~600k prospects (high volume)
   - Rate limit handling (40 req/min)
   - Incremental by modification date
```

### Phase 3: ML Feature Store (Q2 2026)

```
📊 Feature engineering layer
   - Churn prediction features
   - Customer LTV calculation
   - Activity aggregations
```

### Scaling Path

| Trigger | Action |
|---------|--------|
| **>10M rows** | Migrate to Snowflake/Databricks |
| **>5 data sources** | Implement Apache Airflow |
| **Real-time requirements** | Add Kafka/Event Hub streaming |
| **Complex transformations** | Implement dbt |

### Migration to Enterprise Stack

```
Current                          Future
─────────────────────────────    ─────────────────────────────
Task Scheduler      ────────►    Apache Airflow / Azure Data Factory
PostgreSQL          ────────►    Snowflake / Databricks
Python transforms   ────────►    dbt + Python models
Local execution     ────────►    Kubernetes / Azure Container Apps
```

---

## 🔒 Data Quality & Governance

### Built-in Safeguards

| Safeguard | Implementation |
|-----------|----------------|
| **Idempotency** | UPSERT patterns with business keys |
| **Lineage** | Full path from source API to final table |
| **Deduplication** | `ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC)` |
| **Type Safety** | JSONB → typed columns with explicit casting |
| **Null Handling** | `COALESCE` and `NULLIF` for clean data |

### Validation Queries

```sql
-- Check for duplicates
SELECT scope, COUNT(*) - COUNT(DISTINCT deal_id) as duplicates
FROM core.pd_deals GROUP BY scope;

-- Data freshness
SELECT MAX(_updated_at) as last_update FROM core.pd_deals;

-- Orphan records
SELECT COUNT(*) FROM core.pd_deals d
LEFT JOIN core.pd_persons p ON d.person_id = p.person_id AND d.scope = p.scope
WHERE d.person_id IS NOT NULL AND p.person_id IS NULL;
```

---

## 👤 Author

**João V. Cordova** - *Senior Data Engineer*

- 🔗 GitHub: [@joaocordova](https://github.com/joaocordova)
- 🔗 LinkedIn: [João Cordova](https://linkedin.com/in/joaocordova)

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<p align="center">
  <strong>Built with modern data engineering practices for the fitness industry</strong>
</p>
