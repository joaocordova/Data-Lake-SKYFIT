# SkyFit Data Lake - Architecture

## Overview

The SkyFit Data Lake follows the **Medallion Architecture** pattern, a data design pattern used to logically organize data in a lakehouse with the goal of incrementally improving the quality and structure of data as it flows through each layer.

## Layers

### 🥉 Bronze Layer (Raw)

**Location**: Azure Data Lake Storage Gen2

**Purpose**: Store raw, immutable data exactly as received from source systems.

**Characteristics**:
- Append-only (never modified)
- Compressed (.jsonl.gz)
- Partitioned by source, scope, entity, date, run_id
- Full audit trail

**Path Pattern**:
```
bronze/{source}/scope={scope}/entity={entity}/ingestion_date={YYYY-MM-DD}/run_id={timestamp}/part-{N}.jsonl.gz
```

### 🥈 Silver Layer (Staging)

**Location**: PostgreSQL - `stg_*` schemas

**Purpose**: Validated, deduplicated data with full lineage tracking.

**Characteristics**:
- JSONB format (schema-flexible)
- Full lineage (blob_path, line_no)
- Deduplication via UPSERT
- Queryable for debugging

**Tables**:
- `stg_pipedrive.deals_raw`
- `stg_pipedrive.persons_raw`
- `stg_zendesk.tickets_raw`
- etc.

### 🥇 Gold Layer (Core)

**Location**: PostgreSQL - `core` schema

**Purpose**: Analytics-ready, strongly-typed dimensional model.

**Characteristics**:
- Normalized star schema
- Proper data types
- Indexed for performance
- ML-ready

**Tables**:
- `core.pd_deals` (Fact)
- `core.pd_activities` (Fact)
- `core.zd_tickets` (Fact)
- Dimension tables (users, organizations, etc.)

## Data Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Source    │     │   Bronze    │     │   Silver    │     │    Gold     │
│    APIs     │────►│   (ADLS)    │────►│   (STG)     │────►│   (CORE)    │
│             │     │   .jsonl.gz │     │   JSONB     │     │   Typed     │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                    
     Extract          Watermark-based      UPSERT with        JSONB extraction
     via API          incremental          lineage            + type casting
```

## Key Components

### Extractors (Bronze)

| Extractor | Source | Entities |
|-----------|--------|----------|
| `pipedrive_bronze.py` | Pipedrive API | deals, persons, activities, organizations, pipelines, stages, users |
| `zendesk_bronze.py` | Zendesk API | tickets, users, organizations, groups, ticket_fields, ticket_forms |

### Loaders (Silver)

| Loader | Target Schema | Pattern |
|--------|---------------|---------|
| `load_pipedrive_stg.py` | `stg_pipedrive` | UPSERT by (scope, blob_path, line_no) |
| `load_zendesk_stg.py` | `stg_zendesk` | UPSERT by (blob_path, line_no) |

### Transformers (Gold)

| Transformer | Target Tables | Pattern |
|-------------|---------------|---------|
| `normalize_pipedrive.py` | `core.pd_*` | JSONB extraction + UPSERT by business key |
| `normalize_zendesk.py` | `core.zd_*` | JSONB extraction + UPSERT by business key |

## Architecture Decision Records

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-001](decisions/ADR-001-postgresql-as-warehouse.md) | PostgreSQL as Data Warehouse | Accepted |
| [ADR-002](decisions/ADR-002-jsonb-staging-layer.md) | JSONB Staging Layer | Accepted |
| [ADR-003](decisions/ADR-003-multi-tenant-single-table.md) | Multi-Tenant Single Table Design | Accepted |

## Infrastructure

```
┌─────────────────────────────────────────────────────────────────┐
│                        Azure Cloud                               │
│                                                                  │
│  ┌──────────────────────┐    ┌──────────────────────┐          │
│  │  Storage Account     │    │  PostgreSQL Flexible │          │
│  │  (ADLS Gen2)         │    │  Server              │          │
│  │                      │    │                      │          │
│  │  Container: datalake │    │  Database: skyfitevo │          │
│  │  Tier: Cool          │    │  SKU: Burstable B1ms │          │
│  │                      │    │  Storage: 32GB       │          │
│  └──────────────────────┘    └──────────────────────┘          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ VPN/Private Endpoint
                              │
┌─────────────────────────────────────────────────────────────────┐
│                     On-Premises / Local                          │
│                                                                  │
│  ┌──────────────────────┐    ┌──────────────────────┐          │
│  │  Windows Server      │    │  Power BI Desktop    │          │
│  │                      │    │                      │          │
│  │  - Python 3.10+      │    │  DirectQuery to      │          │
│  │  - Task Scheduler    │    │  PostgreSQL          │          │
│  │  - Pipeline scripts  │    │                      │          │
│  └──────────────────────┘    └──────────────────────┘          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Security

| Component | Security Measure |
|-----------|------------------|
| ADLS | Access Key (rotate quarterly) |
| PostgreSQL | SSL required, password auth |
| API Tokens | Environment variables (.env) |
| Credentials | Never committed to git |

## Monitoring

| Metric | Location | Alert Threshold |
|--------|----------|-----------------|
| Pipeline success | `logs/daily_pipeline_*.log` | Any failure |
| Data freshness | `core.*._updated_at` | > 24h stale |
| Record counts | `health_check.ps1` output | > 10% variance |
| Duplicates | Health check queries | Any duplicates |

## Future Architecture

When scale requires, migration path to:

```
Current                              Future
────────────────────────────────     ────────────────────────────────
Windows Task Scheduler          ──►  Apache Airflow / Azure Data Factory
PostgreSQL                      ──►  Snowflake / Databricks
Python transforms               ──►  dbt + Python models  
Local execution                 ──►  Kubernetes / Azure Container Apps
Manual monitoring               ──►  DataDog / Azure Monitor
```
