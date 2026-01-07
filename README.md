# Data-Lake-SKYFIT

<p align="center">
  <img src="docs/assets/logo-placeholder.png" alt="SkyFit Data Lake" width="200"/>
</p>

<h1 align="center">SkyFit Data Lake</h1>

<p align="center">
  <strong>Modern Data Platform for Fitness Industry Analytics</strong>
</p>

<p align="center">
  <a href="#architecture">Architecture</a> •
  <a href="#features">Features</a> •
  <a href="#getting-started">Getting Started</a> •
  <a href="#documentation">Documentation</a> •
  <a href="#contributing">Contributing</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="Python 3.10+"/>
  <img src="https://img.shields.io/badge/PostgreSQL-17-blue.svg" alt="PostgreSQL 17"/>
  <img src="https://img.shields.io/badge/Azure-ADLS%20Gen2-0078D4.svg" alt="Azure ADLS Gen2"/>
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License"/>
</p>

---

## 📋 Overview

**SkyFit Data Lake** is a production-grade data platform that integrates multiple SaaS data sources (CRM, Support, Gym Management) into a unified analytical layer. Built following the **Medallion Architecture** pattern, it provides a scalable foundation for business intelligence and machine learning workloads.

### Business Context

SkyFit is a fitness academy chain that needs to:
- **Unify sales data** from Pipedrive CRM (2 business units: Commercial & Expansion)
- **Consolidate support metrics** from Zendesk
- **Integrate operational data** from EVO gym management system
- **Enable self-service analytics** via Power BI dashboards

### Key Metrics

| Source | Volume | Update Frequency |
|--------|--------|------------------|
| Pipedrive (Commercial) | ~30k deals, ~140k activities | Daily incremental |
| Pipedrive (Expansion) | ~350 deals | Daily incremental |
| Zendesk | ~12k tickets | Daily incremental |
| EVO | TBD | TBD |

---

##  Architecture

<p align="center">
  <img src="docs/architecture/data-flow-diagram.png" alt="Architecture Diagram" width="800"/>
</p>

### Medallion Architecture Implementation

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                    │
├─────────────┬─────────────┬─────────────┬───────────────────────────────────┤
│  Pipedrive  │   Zendesk   │     EVO     │           Future Sources          │
│    (CRM)    │  (Support)  │    (Gym)    │                                   │
└──────┬──────┴──────┬──────┴──────┬──────┴───────────────────────────────────┘
       │             │             │
       ▼             ▼             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER (Raw)                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Azure Data Lake Storage Gen2 (Cool Tier)                           │    │
│  │  Format: .jsonl.gz | Partitioned by: source/scope/entity/date/run   │    │
│  │  Retention: Immutable, versioned by run_id                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER (Staging)                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  PostgreSQL Flexible Server - stg_* schemas                         │    │
│  │  Format: JSONB with full lineage (blob_path, line_no, run_id)       │    │
│  │  Purpose: Deduplication, validation, audit trail                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER (Core)                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  PostgreSQL Flexible Server - core schema                           │    │
│  │  Format: Normalized star schema with proper data types              │    │
│  │  Purpose: Analytics-ready, BI consumption, ML features              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CONSUMPTION                                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Power BI   │  │   Python     │  │   Jupyter    │  │   REST API   │     │
│  │  Dashboards  │  │   ML/AI      │  │  Notebooks   │  │   (Future)   │     │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Layer | Technology | Justification |
|-------|------------|---------------|
| **Ingestion** | Python 3.10+ | Native async support, rich ecosystem for API integrations |
| **Raw Storage** | Azure ADLS Gen2 | Hierarchical namespace, cost-effective Cool tier, native Parquet support |
| **Processing** | Python + psycopg2 | Lightweight, no Spark overhead for current data volumes |
| **Warehouse** | PostgreSQL 17 | JSONB support, mature ecosystem, Azure Flexible Server cost-efficiency |
| **Orchestration** | Windows Task Scheduler | Simple, reliable for single-node deployment |
| **BI** | Power BI | Enterprise standard, DirectQuery to PostgreSQL |

>  For detailed architecture decisions, see [Architecture Decision Records](docs/architecture/decisions/)

---

##  Features

### Data Engineering Capabilities

- ** Incremental Extraction**: Cursor-based and timestamp-based strategies per entity type
- ** Partitioned Storage**: Hive-style partitioning (`scope=X/entity=Y/ingestion_date=Z/run_id=W`)
- ** Full Data Lineage**: Track every record from source API to final table
- ** Idempotent Pipelines**: Safe to re-run without duplicates (UPSERT patterns)
- ** Multi-tenant Support**: Single codebase handles multiple Pipedrive accounts (scopes)
- ** Schema Evolution**: JSONB staging absorbs schema changes without pipeline breaks

### Operational Excellence

- ** Structured Logging**: JSON logs with run metrics for observability
- ** Health Checks**: SQL-based validation queries for data quality
- ** Scheduled Execution**: Windows Task Scheduler integration
- ** Secrets Management**: Environment-based configuration (12-factor app)

---

##  Getting Started

### Prerequisites

- Python 3.10+
- PostgreSQL 17 (local or Azure Flexible Server)
- Azure Storage Account with ADLS Gen2 enabled
- API credentials for Pipedrive and Zendesk

### Quick Start

```bash
# 1. Clone repository
git clone https://github.com/skyfit/data-lake.git
cd data-lake

# 2. Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp config/.env.example config/.env
# Edit config/.env with your credentials

# 5. Initialize database schemas
psql -h <host> -U <user> -d postgres -f sql/schemas/00_create_schemas.sql
psql -h <host> -U <user> -d postgres -f sql/schemas/01_stg_pipedrive.sql
psql -h <host> -U <user> -d postgres -f sql/schemas/02_stg_zendesk.sql
psql -h <host> -U <user> -d postgres -f sql/schemas/03_core_pipedrive.sql
psql -h <host> -U <user> -d postgres -f sql/schemas/04_core_zendesk.sql

# 6. Run extraction (Bronze)
python src/extractors/pipedrive_bronze.py
python src/extractors/zendesk_bronze.py

# 7. Run transformation (Silver → Gold)
.\scripts\run_silver.ps1
```

>  For detailed setup instructions, see [Deployment Guide](docs/deployment/README.md)

---

## 📁 Project Structure

```
skyfit-datalake/
│
├── 📂 config/                    # Configuration management
│   ├── .env.example              # Environment template
│   └── settings.py               # Centralized settings loader
│
├── 📂 docs/                      # Documentation
│   ├── 📂 architecture/          # Architecture diagrams and ADRs
│   ├── 📂 api/                   # API documentation
│   └── 📂 deployment/            # Deployment guides
│
├── 📂 scripts/                   # Automation scripts
│   ├── run_bronze.ps1            # Bronze layer orchestrator
│   ├── run_silver.ps1            # Silver layer orchestrator
│   └── setup_scheduler.ps1       # Task Scheduler setup
│
├── 📂 sql/                       # Database artifacts
│   ├── 📂 schemas/               # DDL scripts
│   └── 📂 validations/           # Data quality queries
│
├── 📂 src/                       # Source code
│   ├── 📂 common/                # Shared utilities
│   │   ├── lake.py               # ADLS client wrapper
│   │   ├── db.py                 # PostgreSQL client wrapper
│   │   └── logging_config.py     # Logging configuration
│   │
│   ├── 📂 extractors/            # Bronze layer (API → ADLS)
│   │   ├── pipedrive_bronze.py   # Pipedrive extractor
│   │   └── zendesk_bronze.py     # Zendesk extractor
│   │
│   ├── 📂 loaders/               # Silver layer (ADLS → STG)
│   │   ├── load_pipedrive_stg.py
│   │   └── load_zendesk_stg.py
│   │
│   └── 📂 transformers/          # Gold layer (STG → CORE)
│       ├── normalize_pipedrive.py
│       └── normalize_zendesk.py
│
├── 📂 tests/                     # Test suite
├── .gitignore
├── requirements.txt
└── README.md
```

---

##  Documentation

| Document | Description |
|----------|-------------|
| [Architecture Overview](docs/architecture/README.md) | High-level system design |
| [Data Flow Specification](docs/architecture/data-flow.md) | Detailed data movement |
| [ADR Index](docs/architecture/decisions/README.md) | Architecture Decision Records |
| [Entity Catalog](docs/data-catalog/README.md) | Data dictionary and schemas |
| [Deployment Guide](docs/deployment/README.md) | Infrastructure setup |
| [Operations Runbook](docs/operations/README.md) | Day-to-day operations |

---

##  Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `AZURE_STORAGE_ACCOUNT` |  Azure Storage Account name |
| `AZURE_STORAGE_KEY` | Storage Account access key |
| `ADLS_CONTAINER` |  Container name (default: `datalake`) |
| `PG_HOST` | PostgreSQL hostname |
| `PG_USER` | PostgreSQL username |
| `PG_PASSWORD` | PostgreSQL password |
| `PIPEDRIVE_COMPANY_DOMAIN` | Pipedrive company subdomain |
| `PIPEDRIVE_TOKEN_COMERCIAL` | API token for Commercial scope |
| `PIPEDRIVE_TOKEN_EXPANSAO` | API token for Expansion scope |
| `ZENDESK_SUBDOMAIN` | Zendesk subdomain |
| `ZENDESK_EMAIL` | Zendesk admin email |
| `ZENDESK_API_TOKEN` | Zendesk API token |

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test module
pytest tests/test_extractors.py -v
```

---

## 📊 Monitoring & Observability

### Logs

All pipeline executions generate structured logs in `logs/` directory:

```
logs/
├── load_pipedrive_stg_20260106T185245Z.json
├── load_zendesk_stg_20260106T185843Z.json
└── normalize_zendesk_2026-01-06.log
```

### Data Quality Checks

SQL-based validation queries in `sql/validations/`:

```sql
-- Check for duplicates
SELECT source_blob_path, source_line_no, COUNT(*)
FROM stg_zendesk.tickets_raw
GROUP BY source_blob_path, source_line_no
HAVING COUNT(*) > 1;

-- Verify record counts
SELECT run_id, COUNT(*) as records
FROM stg_pipedrive.deals_raw
GROUP BY run_id
ORDER BY run_id DESC;
```

---

##  Roadmap

- [x] **Phase 1**: Pipedrive integration (Commercial + Expansion)
- [x] **Phase 2**: Zendesk integration
- [ ] **Phase 3**: EVO gym management integration
- [ ] **Phase 4**: Power BI semantic layer
- [ ] **Phase 5**: Data quality framework (Great Expectations)
- [ ] **Phase 6**: Migration to Azure Data Factory / Airflow

---

##  Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on:

- Code style and standards
- Commit message conventions
- Pull request process
- Development setup


---

##  Authors

- **[João V. Cordova]** - *Data Engineer* - [GitHub](https://github.com/joaocordova)

---

<p align="center">
  Built with ❤️ for the fitness industry
</p>
