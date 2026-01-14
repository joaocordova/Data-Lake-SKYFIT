# SkyFit Data Lake

<p align="center">
  <strong>Modern Data Platform for Fitness Industry Analytics</strong>
</p>

<p align="center">
  <a href="#architecture">Architecture</a> •
  <a href="#data-sources">Data Sources</a> •
  <a href="#getting-started">Getting Started</a> •
  <a href="#documentation">Documentation</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="Python 3.10+"/>
  <img src="https://img.shields.io/badge/PostgreSQL-17-blue.svg" alt="PostgreSQL 17"/>
  <img src="https://img.shields.io/badge/Azure-ADLS%20Gen2-0078D4.svg" alt="Azure ADLS Gen2"/>
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License"/>
</p>

---

##  Overview

**SkyFit Data Lake** is a production-grade data platform that integrates multiple SaaS data sources (CRM, Support, Gym Management) into a unified analytical layer. Built following the **Medallion Architecture** pattern (Bronze → Silver → Gold), it provides a scalable foundation for business intelligence and machine learning workloads.

### Business Context

SkyFit is a fitness academy chain that needs to:
- **Unify operational data** from EVO gym management system (members, sales, entries)
- **Consolidate sales pipeline** from Pipedrive CRM (Commercial & Expansion units)
- **Track support metrics** from Zendesk
- **Enable self-service analytics** via Power BI dashboards

---

##  Data Sources & Volumes

| Source | Entities | Records | Status |
|--------|----------|---------|--------|
| **EVO (W12)** | Members, Sales, Entries, Prospects | **~130M** | ✅ Production |
| **Pipedrive** | Deals, Activities, Persons | ~170K | ✅ Production |
| **Zendesk** | Tickets | ~12K | ✅ Production |

### EVO Detailed Metrics (Main Volume)

| Entity | Records | Bronze | Processing | Update |
|--------|---------|--------|------------|--------|
| **Entries** | ~110M | 45 GB | ~8h extraction | Daily |
| **Sales** | 12.8M | 8 GB | ~2h load (v2) | Daily |
| **Members** | 2.4M + 10M memberships | 3 GB | ~1h load (v2) | Daily |
| **Prospects** | 612K | 500 MB | ~30min | Daily |

---

##  Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                   │
├──────────────┬──────────────┬──────────────┬────────────────────────────────┤
│   EVO (W12)  │   Pipedrive  │    Zendesk   │        Future Sources          │
│  Gym Mgmt    │     CRM      │   Support    │                                │
└──────┬───────┴──────┬───────┴──────┬───────┴────────────────────────────────┘
       │              │              │
       ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER (Raw)                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Azure Data Lake Storage Gen2                                       │    │
│  │  Format: JSONL.GZ compressed | Partitioned by: entity/date/run_id   │    │
│  │  Size: ~60 GB | Retention: Immutable, versioned by run_id           │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER (STG)                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  PostgreSQL Flexible Server - stg_* schemas                         │    │
│  │  Format: JSONB raw_data with full lineage (source, run_id, line)    │    │
│  │  Size: ~120 GB | Purpose: Deduplication, validation, audit trail    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER (CORE)                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  PostgreSQL Flexible Server - core schema                           │    │
│  │  Format: Normalized star schema with proper data types              │    │
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
| **Extraction** | Python + multiprocessing | Parallel API calls, rate limiting |
| **Raw Storage** | Azure ADLS Gen2 | Cost-effective, hierarchical namespace |
| **Processing** | Python + psycopg2 COPY | Bulk loading 50-100x faster than INSERT |
| **Warehouse** | PostgreSQL 17 Flexible | JSONB support, cost-efficient (~$85/month) |
| **Orchestration** | Windows Task Scheduler | Simple, reliable for current scale |
| **BI** | Power BI | Enterprise standard, DirectQuery |

---

##  Performance Optimizations (v2)

The EVO pipeline includes optimized v2 scripts with significant performance improvements:

| Optimization | Description | Gain |
|--------------|-------------|------|
| **ThreadedConnectionPool** | Reuses PostgreSQL connections | 20-30% |
| **Batch Processing** | Groups 5-10 files per COPY | 2-3x |
| **Full-Refresh Mode** | TRUNCATE + INSERT without indexes | 5-10x |
| **Exponential Backoff** | Survives Azure failovers | 100% reliability |

### Benchmark Results

| Operation | v1 | v2 | Improvement |
|-----------|-----|-----|-------------|
| Sales Load (12.8M) | 6h | 2h | **3x** |
| Sales Transform | 2h | 30min | **4x** |
| Members Load (2.4M) | 3h | 1h | **3x** |
| Members Transform | 4.6h | 45min | **6x** |

---

##  Infrastructure Costs

| Resource | Configuration | Monthly Cost |
|----------|---------------|--------------|
| Azure Data Lake Gen2 | ~60 GB | ~$5 |
| Azure PostgreSQL Flexible | B2ms, 256 GB | ~$85 |
| **Total** | | **~$90/month** |

**Savings vs alternatives**: ~$260-400/month compared to Azure Synapse or Snowflake.

---

## 📁 Project Structure

```
skyfit-datalake/
├── config/                         # Configuration
│   ├── .env.example
│   └── settings.py
├── docs/                           # Documentation
│   ├── architecture/               # Architecture & ADRs
│   ├── data-catalog/               # Entity schemas
│   ├── evo/                        # EVO-specific docs
│   │   ├── EVO_DATA_MODEL.md
│   │   ├── PERFORMANCE_TUNING.md
│   │   └── ARCHITECTURE_DECISIONS.md
│   └── OPERATIONS_GUIDE.md
├── sql/                            # Database artifacts
│   ├── schemas/
│   │   ├── 00_create_schemas.sql
│   │   ├── 01_stg_pipedrive.sql
│   │   ├── 02_stg_zendesk.sql
│   │   └── evo/                    # EVO schemas
│   │       ├── evo_schemas.sql
│   │       ├── add_missing_columns.sql
│   │       └── optimize_storage.py
│   └── validations/
├── src/                            # Source code
│   ├── common/                     # Shared utilities
│   ├── extractors/                 # Pipedrive & Zendesk
│   ├── loaders/                    # Pipedrive & Zendesk
│   ├── transformers/               # Pipedrive & Zendesk
│   └── evo/                        # EVO Pipeline
│       ├── extractors/
│       ├── loaders/                # v2 optimized
│       ├── transformers/           # v2 with full-refresh
│       └── utils/
├── scripts/                        # Automation
├── requirements.txt
└── README.md
```

---

##  Getting Started

### Prerequisites

```bash
pip install -r requirements.txt
```

### Configuration

```bash
cp config/.env.example config/.env
# Edit with your credentials
```

### Initialize Database

```bash
psql -h $PG_HOST -U $PG_USER -d $PG_DATABASE -f sql/schemas/00_create_schemas.sql
psql -h $PG_HOST -U $PG_USER -d $PG_DATABASE -f sql/schemas/evo/evo_schemas.sql
```

### Run EVO Pipeline

```powershell
# 1. Extract (API → Bronze)
python src/evo/extractors/evo_members_bronze_parallel.py --workers 8

# 2. Load (Bronze → STG) - v2 optimized
python src/evo/loaders/load_evo_members_stg_fast_v2.py --workers 8 --batch-size 10 --all-runs

# 3. Transform (STG → CORE) - full refresh
python src/evo/transformers/normalize_evo_members_v2.py --full-refresh
```

---

## 📖 Documentation

| Document | Description |
|----------|-------------|
| [Architecture Overview](docs/architecture/README.md) | System design & data flow |
| [EVO Data Model](docs/evo/EVO_DATA_MODEL.md) | Complete EVO schema & ERD |
| [Performance Tuning](docs/evo/PERFORMANCE_TUNING.md) | Optimization guide |
| [Architecture Decisions](docs/evo/ARCHITECTURE_DECISIONS.md) | ADRs & trade-offs |
| [Data Catalog](docs/data-catalog/README.md) | Entity dictionary |
| [Operations Guide](docs/OPERATIONS_GUIDE.md) | Day-to-day operations |

---

## 🗄️ Core Data Model (EVO)

### Entity Relationship Diagram

```
                                    ┌─────────────────┐
                                    │  evo_prospects  │
                                    │    (leads)      │
                                    └────────┬────────┘
                                             │ converts to
                                             ▼
┌─────────────────┐    1:N    ┌─────────────────────────┐    N:1    ┌─────────────────┐
│   evo_branches  │◄──────────│      evo_members        │──────────►│  evo_employees  │
│   (unidades)    │           │      (clientes)         │           │  (consultants)  │
└────────┬────────┘           └───────────┬─────────────┘           └─────────────────┘
         │                                │
         │                    ┌───────────┼───────────┐
         │                    │           │           │
         │                    ▼           ▼           ▼
         │           ┌──────────────┐ ┌─────────┐ ┌────────────────────┐
         │           │evo_member_   │ │evo_     │ │evo_member_         │
         │           │memberships   │ │entries  │ │contacts            │
         │           │(contratos)   │ │(acessos)│ │(telefones/emails)  │
         │           └──────┬───────┘ └─────────┘ └────────────────────┘
         │                  │
         │                  │ N:1
         │                  ▼
         │           ┌─────────────┐
         └──────────►│  evo_sales  │
                     │  (vendas)   │
                     └──────┬──────┘
                            │
                ┌───────────┴───────────┐
                │                       │
                ▼                       ▼
       ┌──────────────┐        ┌───────────────┐
       │evo_sale_items│        │evo_receivables│
       │  (produtos)  │        │  (parcelas)   │
       └──────────────┘        └───────────────┘
```

### Key Tables

| Table | Records | Description |
|-------|---------|-------------|
| `core.evo_members` | 2.4M | Gym members with contact info |
| `core.evo_member_memberships` | 10M | Membership contracts |
| `core.evo_sales` | 12.8M | Sales transactions |
| `core.evo_entries` | 110M | Gym access logs |
| `core.evo_prospects` | 612K | Leads |

---

##  Operations

### Storage Optimization

```bash
python sql/schemas/evo/optimize_storage.py --analyze
python sql/schemas/evo/optimize_storage.py --vacuum --reindex
```

### Troubleshooting

| Error | Solution |
|-------|----------|
| `column does not exist` | Run `sql/schemas/evo/add_missing_columns.sql` |
| `read-only transaction` | Azure failover - wait 30-60s (v2 auto-retries) |
| `storage full` | Run `optimize_storage.py --vacuum --reindex` |

---

##  Changelog

### 2026-01-14 - EVO Pipeline 
- ✅ **EVO Integration Complete**: Members, Sales, Entries, Prospects (~130M records)
- ✅ **Performance Optimized**: Scripts v2 with ThreadedConnectionPool (3-6x speedup)
- ✅ **Full-Refresh Mode**: TRUNCATE + INSERT without indexes (5-10x faster)
- ✅ **New Fields**: `code_totalpass`, `user_id_gurupass` in Members
- ✅ **Resilience**: Exponential backoff retry for Azure failovers
- ✅ **Storage Tools**: VACUUM/REINDEX optimization scripts
- ✅ **Documentation**: Complete EVO data model, ADRs, performance guide

### Previous
- ✅ Pipedrive integration (Commercial + Expansion)
- ✅ Zendesk integration
- ✅ Medallion architecture implementation
- ✅ JSONB staging layer

---

## 👤 Author

**João V. Cordova** - Data Engineer - [GitHub](https://github.com/joaocordova)

---

<p align="center">Built with ❤️ for the fitness industry</p>
