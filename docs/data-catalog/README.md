# SkyFit Data Lake - Data Catalog

## Overview

This catalog documents all tables in the SkyFit Data Lake, organized by layer and source system.

---

## Layer Architecture

| Layer | Schema | Purpose | Format |
|-------|--------|---------|--------|
| **Bronze** | ADLS (files) | Raw immutable data | .jsonl.gz |
| **Silver** | `stg_pipedrive`, `stg_zendesk` | Validated staging | JSONB |
| **Gold** | `core` | Analytics-ready | Typed columns |

---

## Pipedrive (CRM)

### Source Information

| Property | Value |
|----------|-------|
| **System** | Pipedrive CRM |
| **API Version** | v1 |
| **Scopes** | comercial, expansao |
| **Update Frequency** | Daily incremental |
| **Extraction Method** | Watermark-based (update_time) |

### Gold Layer Tables

#### `core.pd_deals` (Fact Table)

Sales opportunities/deals from Pipedrive.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `deal_id` | BIGINT | Primary key (from Pipedrive) | 12345 |
| `scope` | VARCHAR(50) | Business unit | 'comercial' |
| `title` | VARCHAR(500) | Deal name | 'Plano Premium - João Silva' |
| `value` | NUMERIC(15,2) | Deal value in BRL | 1500.00 |
| `currency` | VARCHAR(10) | Currency code | 'BRL' |
| `status` | VARCHAR(20) | Current status | 'open', 'won', 'lost' |
| `pipeline_id` | BIGINT | FK to pd_pipelines | 1 |
| `stage_id` | BIGINT | FK to pd_stages | 5 |
| `person_id` | BIGINT | FK to pd_persons | 789 |
| `org_id` | BIGINT | FK to pd_organizations | 456 |
| `user_id` | BIGINT | FK to pd_users (owner) | 12 |
| `add_time` | TIMESTAMPTZ | Created timestamp | 2026-01-01 10:00:00 |
| `update_time` | TIMESTAMPTZ | Last modified | 2026-01-07 15:30:00 |
| `won_time` | TIMESTAMPTZ | When marked as won | 2026-01-05 14:00:00 |
| `lost_time` | TIMESTAMPTZ | When marked as lost | NULL |
| `close_time` | TIMESTAMPTZ | When closed (won or lost) | 2026-01-05 14:00:00 |
| `expected_close_date` | DATE | Expected closing date | 2026-01-15 |
| `lost_reason` | TEXT | Reason for loss | 'Price too high' |
| `visible_to` | VARCHAR(20) | Visibility setting | 'everyone' |
| `custom_fields` | JSONB | Additional Pipedrive fields | {"source": "website"} |
| `_source_run_id` | VARCHAR(20) | ETL batch ID | '20260107T060000Z' |
| `_loaded_at` | TIMESTAMPTZ | First load timestamp | 2026-01-07 06:05:00 |
| `_updated_at` | TIMESTAMPTZ | Last update timestamp | 2026-01-07 19:56:00 |

**Primary Key**: `(deal_id, scope)`

**Indexes**:
- `idx_pd_deals_scope` - Filter by business unit
- `idx_pd_deals_status` - Filter by status
- `idx_pd_deals_pipeline` - Join to pipeline
- `idx_pd_deals_stage` - Join to stage
- `idx_pd_deals_person` - Join to person
- `idx_pd_deals_user` - Filter by owner
- `idx_pd_deals_add_time` - Time-based queries
- `idx_pd_deals_update_time` - Incremental processing

**Record Count**: ~31,000

---

#### `core.pd_persons` (Dimension)

Contacts/leads in Pipedrive.

| Column | Type | Description |
|--------|------|-------------|
| `person_id` | BIGINT | Primary key |
| `scope` | VARCHAR(50) | Business unit |
| `name` | VARCHAR(500) | Full name |
| `primary_email` | VARCHAR(255) | Main email |
| `primary_phone` | VARCHAR(100) | Main phone |
| `org_id` | BIGINT | FK to organizations |
| `owner_id` | BIGINT | FK to users |
| `emails` | JSONB | All emails array |
| `phones` | JSONB | All phones array |
| `add_time` | TIMESTAMPTZ | Created |
| `update_time` | TIMESTAMPTZ | Modified |
| `custom_fields` | JSONB | Additional fields |

**Primary Key**: `(person_id, scope)`

**Record Count**: ~44,000

---

#### `core.pd_activities` (Fact Table)

Tasks, calls, meetings, emails in Pipedrive.

| Column | Type | Description |
|--------|------|-------------|
| `activity_id` | BIGINT | Primary key |
| `scope` | VARCHAR(50) | Business unit |
| `type` | VARCHAR(100) | Activity type (call, meeting, task, email) |
| `subject` | VARCHAR(500) | Activity subject |
| `done` | BOOLEAN | Completion status |
| `due_date` | DATE | Scheduled date |
| `due_time` | TEXT | Scheduled time |
| `duration` | TEXT | Duration |
| `deal_id` | BIGINT | FK to deals |
| `person_id` | BIGINT | FK to persons |
| `org_id` | BIGINT | FK to organizations |
| `user_id` | BIGINT | FK to users (assigned) |
| `add_time` | TIMESTAMPTZ | Created |
| `update_time` | TIMESTAMPTZ | Modified |
| `marked_as_done_time` | TIMESTAMPTZ | Completion time |
| `note` | TEXT | Activity notes |

**Primary Key**: `(activity_id, scope)`

**Record Count**: ~141,000

---

#### `core.pd_organizations` (Dimension)

Companies/accounts in Pipedrive.

| Column | Type | Description |
|--------|------|-------------|
| `org_id` | BIGINT | Primary key |
| `scope` | VARCHAR(50) | Business unit |
| `name` | VARCHAR(500) | Company name |
| `address` | TEXT | Full address |
| `owner_id` | BIGINT | FK to users |
| `add_time` | TIMESTAMPTZ | Created |
| `update_time` | TIMESTAMPTZ | Modified |
| `custom_fields` | JSONB | Additional fields |

**Primary Key**: `(org_id, scope)`

**Record Count**: ~10,700

---

#### `core.pd_pipelines` (Dimension)

Sales pipelines configuration.

| Column | Type | Description |
|--------|------|-------------|
| `pipeline_id` | BIGINT | Primary key |
| `scope` | VARCHAR(50) | Business unit |
| `name` | VARCHAR(255) | Pipeline name |
| `active` | BOOLEAN | Is active |
| `order_nr` | INTEGER | Display order |
| `add_time` | TIMESTAMPTZ | Created |
| `update_time` | TIMESTAMPTZ | Modified |

**Primary Key**: `(pipeline_id, scope)`

**Record Count**: 9

---

#### `core.pd_stages` (Dimension)

Pipeline stages configuration.

| Column | Type | Description |
|--------|------|-------------|
| `stage_id` | BIGINT | Primary key |
| `scope` | VARCHAR(50) | Business unit |
| `pipeline_id` | BIGINT | FK to pipelines |
| `name` | VARCHAR(255) | Stage name |
| `order_nr` | INTEGER | Position in pipeline |
| `active_flag` | BOOLEAN | Is active |
| `deal_probability` | INTEGER | Win probability % |
| `add_time` | TIMESTAMPTZ | Created |
| `update_time` | TIMESTAMPTZ | Modified |

**Primary Key**: `(stage_id, scope)`

**Record Count**: 43

---

#### `core.pd_users` (Dimension)

Pipedrive users (sales reps).

| Column | Type | Description |
|--------|------|-------------|
| `user_id` | BIGINT | Primary key |
| `scope` | VARCHAR(50) | Business unit |
| `name` | VARCHAR(255) | Full name |
| `email` | VARCHAR(255) | Email address |
| `active_flag` | BOOLEAN | Is active |
| `role_id` | BIGINT | Role identifier |
| `created` | TIMESTAMPTZ | Created |
| `modified` | TIMESTAMPTZ | Modified |

**Primary Key**: `(user_id, scope)`

**Record Count**: 20

---

## Zendesk (Support)

### Source Information

| Property | Value |
|----------|-------|
| **System** | Zendesk Support |
| **API Version** | v2 |
| **Update Frequency** | Daily incremental |
| **Extraction Method** | Cursor-based pagination |

### Gold Layer Tables

#### `core.zd_tickets` (Fact Table)

Support tickets from Zendesk.

| Column | Type | Description |
|--------|------|-------------|
| `ticket_id` | BIGINT | Primary key |
| `subject` | VARCHAR(500) | Ticket subject |
| `description` | TEXT | Full description |
| `status` | VARCHAR(50) | new, open, pending, solved, closed |
| `priority` | VARCHAR(20) | low, normal, high, urgent |
| `type` | VARCHAR(50) | question, incident, problem, task |
| `requester_id` | BIGINT | FK to users (customer) |
| `submitter_id` | BIGINT | FK to users |
| `assignee_id` | BIGINT | FK to users (agent) |
| `group_id` | BIGINT | FK to groups |
| `organization_id` | BIGINT | FK to organizations |
| `via_channel` | VARCHAR(100) | Channel (email, web, api) |
| `via_source` | JSONB | Source details |
| `tags` | JSONB | Tag array |
| `custom_fields` | JSONB | Custom field values |
| `created_at` | TIMESTAMPTZ | Created |
| `updated_at` | TIMESTAMPTZ | Modified |
| `due_at` | TIMESTAMPTZ | Due date |
| `satisfaction_rating` | JSONB | CSAT data |

**Primary Key**: `ticket_id`

**Record Count**: ~12,000

---

#### `core.zd_ticket_tags` (Bridge Table)

Normalized ticket tags for analytics.

| Column | Type | Description |
|--------|------|-------------|
| `ticket_id` | BIGINT | FK to tickets |
| `tag` | VARCHAR(255) | Tag value |
| `_loaded_at` | TIMESTAMPTZ | Load timestamp |

**Primary Key**: `(ticket_id, tag)`

**Record Count**: ~10,200

---

#### `core.zd_ticket_custom_fields` (Bridge Table)

Normalized custom field values.

| Column | Type | Description |
|--------|------|-------------|
| `ticket_id` | BIGINT | FK to tickets |
| `field_id` | BIGINT | FK to ticket_fields |
| `value` | TEXT | Field value |
| `_loaded_at` | TIMESTAMPTZ | Load timestamp |

**Primary Key**: `(ticket_id, field_id)`

**Record Count**: ~30,000

---

#### `core.zd_users` (Dimension)

Zendesk users (agents and customers).

| Column | Type | Description |
|--------|------|-------------|
| `user_id` | BIGINT | Primary key |
| `name` | VARCHAR(255) | Full name |
| `email` | VARCHAR(255) | Email |
| `role` | VARCHAR(50) | admin, agent, end-user |
| `organization_id` | BIGINT | FK to organizations |
| `active` | BOOLEAN | Is active |
| `verified` | BOOLEAN | Email verified |
| `suspended` | BOOLEAN | Is suspended |
| `created_at` | TIMESTAMPTZ | Created |
| `updated_at` | TIMESTAMPTZ | Modified |
| `user_fields` | JSONB | Custom fields |

**Primary Key**: `user_id`

**Record Count**: ~1,400

---

#### `core.zd_organizations` (Dimension)

Customer organizations in Zendesk.

| Column | Type | Description |
|--------|------|-------------|
| `organization_id` | BIGINT | Primary key |
| `name` | VARCHAR(255) | Organization name |
| `domain_names` | JSONB | Associated domains |
| `group_id` | BIGINT | Default group |
| `shared_tickets` | BOOLEAN | Ticket sharing enabled |
| `shared_comments` | BOOLEAN | Comment sharing enabled |
| `external_id` | VARCHAR(255) | External system ID |
| `tags` | JSONB | Tags |
| `organization_fields` | JSONB | Custom fields |
| `created_at` | TIMESTAMPTZ | Created |
| `updated_at` | TIMESTAMPTZ | Modified |

**Primary Key**: `organization_id`

**Record Count**: 4

---

#### `core.zd_groups` (Dimension)

Support groups/teams.

| Column | Type | Description |
|--------|------|-------------|
| `group_id` | BIGINT | Primary key |
| `name` | VARCHAR(255) | Group name |
| `description` | TEXT | Description |
| `default_flag` | BOOLEAN | Is default |
| `deleted` | BOOLEAN | Is deleted |
| `created_at` | TIMESTAMPTZ | Created |
| `updated_at` | TIMESTAMPTZ | Modified |

**Primary Key**: `group_id`

**Record Count**: 14

---

## Data Lineage

Every record in Gold layer can be traced back to source:

```
core.pd_deals.deal_id = 12345
    ↓
stg_pipedrive.deals_raw (source_blob_path, source_line_no)
    ↓
bronze/pipedrive/scope=comercial/entity=deals/ingestion_date=2026-01-07/run_id=20260107T060000Z/part-00001.jsonl.gz:line 42
    ↓
Pipedrive API /v1/deals?id=12345
```

---

## Update Patterns

| Table | Update Pattern | Deduplication |
|-------|---------------|---------------|
| `pd_deals` | UPSERT by (deal_id, scope) | Latest by update_time |
| `pd_activities` | UPSERT by (activity_id, scope) | Latest by update_time |
| `zd_tickets` | UPSERT by ticket_id | Latest by updated_at |
| `zd_ticket_tags` | DELETE + INSERT per ticket | Dedupe in batch |

---

## Common Queries

### Deal Funnel by Scope

```sql
SELECT 
    scope,
    pipeline_id,
    stage_id,
    COUNT(*) as deals,
    SUM(value) as total_value
FROM core.pd_deals
WHERE status = 'open'
GROUP BY scope, pipeline_id, stage_id
ORDER BY scope, pipeline_id, stage_id;
```

### Support Volume by Channel

```sql
SELECT 
    via_channel,
    DATE_TRUNC('week', created_at) as week,
    COUNT(*) as tickets,
    AVG(EXTRACT(EPOCH FROM (updated_at - created_at))/3600) as avg_hours_to_update
FROM core.zd_tickets
WHERE created_at >= NOW() - INTERVAL '30 days'
GROUP BY via_channel, week
ORDER BY week DESC, tickets DESC;
```

### Cross-System: Deals with Support Tickets

```sql
-- Requires email matching between systems
SELECT 
    d.deal_id,
    d.title,
    d.value,
    COUNT(t.ticket_id) as support_tickets
FROM core.pd_deals d
JOIN core.pd_persons p ON d.person_id = p.person_id AND d.scope = p.scope
LEFT JOIN core.zd_users zu ON LOWER(p.primary_email) = LOWER(zu.email)
LEFT JOIN core.zd_tickets t ON zu.user_id = t.requester_id
WHERE d.status = 'won'
GROUP BY d.deal_id, d.title, d.value
HAVING COUNT(t.ticket_id) > 0
ORDER BY support_tickets DESC;
```
