# EVO Data Model

Complete data model documentation for the EVO (W12) gym management system integration.

## Overview

The EVO integration follows a three-layer architecture:

| Layer | Schema | Purpose | Format |
|-------|--------|---------|--------|
| **Bronze** | Azure ADLS | Raw immutable data | JSONL.GZ |
| **Silver (STG)** | `stg_evo.*` | Staging with JSONB | PostgreSQL |
| **Gold (CORE)** | `core.*` | Normalized analytics | PostgreSQL |

---

## Entity Relationship Diagram

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
         │           ┌──────────────┐ ┌────────┐ ┌────────────────────┐
         │           │evo_member_   │ │evo_    │ │evo_member_         │
         │           │memberships   │ │entries │ │contacts            │
         │           │(contratos)   │ │(acessos)│ │(telefones/emails) │
         │           └──────┬───────┘ └────────┘ └────────────────────┘
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

---

## STG Layer (Silver)

### stg_evo.prospects_raw

| Column | Type | Description |
|--------|------|-------------|
| prospect_id | BIGINT | Primary key |
| raw_data | JSONB | Complete JSON from API |
| _source_file | TEXT | Bronze file path |
| run_id | TEXT | Extraction run ID |
| ingestion_date | DATE | Extraction date |
| _loaded_at | TIMESTAMPTZ | Load timestamp |
| _updated_at | TIMESTAMPTZ | Last update |

### stg_evo.sales_raw

| Column | Type | Description |
|--------|------|-------------|
| sale_id | BIGINT | Primary key |
| raw_data | JSONB | Complete JSON from API |
| _source_file | TEXT | Bronze file path |
| run_id | TEXT | Extraction run ID |
| ingestion_date | DATE | Extraction date |
| _loaded_at | TIMESTAMPTZ | Load timestamp |
| _updated_at | TIMESTAMPTZ | Last update |

### stg_evo.members_raw

| Column | Type | Description |
|--------|------|-------------|
| member_id | BIGINT | Primary key |
| raw_data | JSONB | Complete JSON from API |
| _source_file | TEXT | Bronze file path |
| run_id | TEXT | Extraction run ID |
| ingestion_date | DATE | Extraction date |
| _loaded_at | TIMESTAMPTZ | Load timestamp |
| _updated_at | TIMESTAMPTZ | Last update |

### stg_evo.entries_raw

| Column | Type | Description |
|--------|------|-------------|
| entry_id | TEXT | MD5 hash (deterministic PK) |
| raw_data | JSONB | Complete JSON from API |
| _source_file | TEXT | Bronze file path |
| run_id | TEXT | Extraction run ID |
| ingestion_date | DATE | Extraction date |
| _loaded_at | TIMESTAMPTZ | Load timestamp |
| _updated_at | TIMESTAMPTZ | Last update |

> **Note**: Entries lacks a natural primary key in the API. We generate a deterministic `entry_id` using MD5 hash of key fields: `MD5(member_id || branch_id || entry_date || register_date)`.

---

## CORE Layer (Gold)

### core.evo_prospects

| Column | Type | Source JSON Path | Description |
|--------|------|------------------|-------------|
| prospect_id | BIGINT PK | idProspect | Unique ID |
| branch_id | BIGINT | idBranch | Branch reference |
| first_name | TEXT | firstName | First name |
| last_name | TEXT | lastName | Last name |
| email | TEXT | email | Email address |
| cellphone | TEXT | telephone | Phone number |
| document | TEXT | document | CPF |
| gender | TEXT | gender | M/F |
| status | TEXT | currentStatus | Lead status |
| origin | TEXT | origin | Lead source |
| register_date | TIMESTAMPTZ | registerDate | Creation date |
| conversion_date | TIMESTAMPTZ | conversionDate | When converted to member |
| employee_consultant_id | BIGINT | idEmployeeConsultant | Sales rep |

### core.evo_members

| Column | Type | Source JSON Path | Description |
|--------|------|------------------|-------------|
| member_id | BIGINT PK | idMember | Unique ID |
| branch_id | BIGINT | idBranch | Home branch |
| branch_name | TEXT | branchName | Branch name |
| first_name | TEXT | firstName | First name |
| last_name | TEXT | lastName | Last name |
| full_name | TEXT | (generated) | Concatenated name |
| register_name | TEXT | registerName | Legal name |
| document | TEXT | document | CPF |
| document_id | TEXT | documentId | RG |
| email | TEXT | contacts[type=Email] | Primary email |
| cellphone | TEXT | contacts[type=Cellphone] | Primary phone |
| gender | TEXT | gender | M/F |
| birth_date | DATE | birthDate | Birth date |
| address | TEXT | address | Street |
| address_number | TEXT | number | Number |
| neighborhood | TEXT | neighborhood | Neighborhood |
| city | TEXT | city | City |
| state | TEXT | state | State (UF) |
| zip_code | TEXT | zipCode | CEP |
| status | TEXT | status | Active/Inactive |
| membership_status | TEXT | membershipStatus | Current plan status |
| access_blocked | BOOLEAN | accessBlocked | Access restriction |
| blocked_reason | TEXT | blockedReason | Block reason |
| gympass_id | TEXT | gympassId | Gympass integration |
| **code_totalpass** | TEXT | **codeTotalpass** | **Totalpass integration** |
| user_id_gurupass | TEXT | userIdGurupass | Gurupass integration |
| personal_trainer | BOOLEAN | personalTrainer | Is personal trainer |
| register_date | TIMESTAMPTZ | registerDate | Registration date |
| conversion_date | TIMESTAMPTZ | conversionDate | Prospect conversion |
| last_access_date | TIMESTAMPTZ | lastAccessDate | Last gym entry |
| employee_consultant_id | BIGINT | idEmployeeConsultant | Sales consultant |
| employee_instructor_id | BIGINT | idEmployeeInstructor | Assigned instructor |

### core.evo_member_memberships

| Column | Type | Source JSON Path | Description |
|--------|------|------------------|-------------|
| member_membership_id | BIGINT PK | idMemberMembership | Contract ID |
| member_id | BIGINT FK | (parent) | Member reference |
| membership_id | BIGINT | idMembership | Plan type ID |
| membership_name | TEXT | name | Plan name |
| start_date | DATE | startDate | Contract start |
| end_date | DATE | endDate | Contract end |
| membership_status | TEXT | membershipStatus | active/expired/canceled |
| cancel_date | TIMESTAMPTZ | cancelDate | Cancellation date |
| sale_id | BIGINT | idSale | Related sale |
| sale_date | TIMESTAMPTZ | saleDate | Original sale date |
| value_next_month | DECIMAL | valueNextMonth | Next charge amount |
| loyalty_end_date | DATE | loyaltyEndDate | Minimum stay date |

### core.evo_member_contacts

| Column | Type | Source JSON Path | Description |
|--------|------|------------------|-------------|
| contact_id | BIGINT PK | idPhone | Contact ID |
| member_id | BIGINT FK | idMember | Member reference |
| contact_type | TEXT | contactType | Cellphone/Email/Phone |
| description | TEXT | description | Contact value |
| ddi | TEXT | ddi | Country code |

### core.evo_sales

| Column | Type | Source JSON Path | Description |
|--------|------|------------------|-------------|
| sale_id | BIGINT PK | idSale | Sale ID |
| branch_id | BIGINT | idBranch | Branch |
| member_id | BIGINT | idMember | Customer |
| prospect_id | BIGINT | idProspect | Lead (if not converted) |
| sale_date | TIMESTAMPTZ | saleDate | Sale timestamp |
| total_value | DECIMAL(15,2) | totalValue | Total amount |
| discount_value | DECIMAL(15,2) | discountValue | Discounts applied |
| payment_status | TEXT | servicePaymentStatus | Payment status |
| removed | BOOLEAN | flRemoved | Cancelled sale |
| employee_id | BIGINT | idEmployee | Salesperson |

### core.evo_sale_items

| Column | Type | Source JSON Path | Description |
|--------|------|------------------|-------------|
| sale_item_id | BIGINT PK | idSaleItem | Item ID |
| sale_id | BIGINT FK | idSale | Parent sale |
| item_name | TEXT | itemName | Product/service name |
| item_type | TEXT | itemType | Product type |
| quantity | INT | quantity | Quantity |
| unit_value | DECIMAL(15,2) | value | Unit price |
| total_value | DECIMAL(15,2) | totalValue | Line total |

### core.evo_receivables

| Column | Type | Source JSON Path | Description |
|--------|------|------------------|-------------|
| receivable_id | BIGINT PK | idReceivable | Receivable ID |
| sale_id | BIGINT FK | idSale | Parent sale |
| due_date | DATE | dueDate | Due date |
| amount | DECIMAL(15,2) | ammount | Amount due |
| amount_paid | DECIMAL(15,2) | ammountPaid | Amount received |
| payment_date | DATE | paymentDate | Payment date |
| status | TEXT | status | open/paid/overdue |
| payment_method | TEXT | paymentMethodName | Card/PIX/etc |

### core.evo_entries

| Column | Type | Source JSON Path | Description |
|--------|------|------------------|-------------|
| entry_id | TEXT PK | (MD5 hash) | Deterministic ID |
| member_id | BIGINT | idMember | Member |
| branch_id | BIGINT | idBranch | Branch |
| entry_date | TIMESTAMPTZ | entryDate | Entry timestamp |
| register_date | TIMESTAMPTZ | registerDate | Record creation |
| device | TEXT | device | Access device (turnstile) |
| entry_action | TEXT | action | Entry/Exit |
| membership_id | BIGINT | idMembership | Active membership |

---

## Indexes

### STG Layer
```sql
-- Primary keys with automatic indexes
CREATE UNIQUE INDEX ON stg_evo.prospects_raw(prospect_id);
CREATE UNIQUE INDEX ON stg_evo.sales_raw(sale_id);
CREATE UNIQUE INDEX ON stg_evo.members_raw(member_id);
CREATE UNIQUE INDEX ON stg_evo.entries_raw(entry_id);
```

### CORE Layer
```sql
-- Members
CREATE INDEX idx_core_members_branch ON core.evo_members(branch_id);
CREATE INDEX idx_core_members_status ON core.evo_members(status);
CREATE INDEX idx_core_members_last_access ON core.evo_members(last_access_date);

-- Memberships
CREATE INDEX idx_core_memberships_member ON core.evo_member_memberships(member_id);
CREATE INDEX idx_core_memberships_status ON core.evo_member_memberships(membership_status);

-- Sales
CREATE INDEX idx_core_sales_member ON core.evo_sales(member_id);
CREATE INDEX idx_core_sales_branch ON core.evo_sales(branch_id);
CREATE INDEX idx_core_sales_date ON core.evo_sales(sale_date);

-- Entries (partitioned by year)
CREATE INDEX idx_core_entries_member ON core.evo_entries(member_id);
CREATE INDEX idx_core_entries_branch ON core.evo_entries(branch_id);
CREATE INDEX idx_core_entries_date ON core.evo_entries(entry_date);
```

---

## Data Quality Rules

### Deduplication
- **STG**: UPSERT on primary key, keeps latest version
- **CORE**: Same, with `_updated_at` tracking

### Null Handling
- Empty strings converted to NULL
- Boolean defaults: FALSE for flags
- Numeric defaults: 0 or NULL based on context

### Date Handling
- All timestamps stored as TIMESTAMPTZ (UTC)
- Date-only fields stored as DATE
- Invalid dates converted to NULL

### Entry ID Generation

Since Entries API lacks a primary key, we generate one:

```python
import hashlib

def generate_entry_id(member_id, branch_id, entry_date, register_date):
    key = f"{member_id}|{branch_id}|{entry_date}|{register_date}"
    return hashlib.md5(key.encode()).hexdigest()
```

This ensures:
- Deterministic: same record always gets same ID
- Collision-resistant: different records get different IDs
- Idempotent: re-processing doesn't create duplicates

---

## Sample Queries

### Active Members by Branch
```sql
SELECT 
    branch_name,
    COUNT(*) AS active_members
FROM core.evo_members
WHERE status = 'Active'
GROUP BY branch_name
ORDER BY active_members DESC;
```

### Revenue by Month
```sql
SELECT 
    DATE_TRUNC('month', sale_date) AS month,
    SUM(total_value) AS revenue,
    COUNT(DISTINCT member_id) AS customers
FROM core.evo_sales
WHERE NOT removed
GROUP BY 1
ORDER BY 1 DESC;
```

### Member Retention (Active Memberships)
```sql
SELECT 
    m.branch_name,
    COUNT(DISTINCT mm.member_id) AS members_with_active_plan
FROM core.evo_member_memberships mm
JOIN core.evo_members m ON mm.member_id = m.member_id
WHERE mm.membership_status = 'active'
GROUP BY 1;
```

### Daily Entries
```sql
SELECT 
    DATE(entry_date) AS day,
    COUNT(*) AS entries,
    COUNT(DISTINCT member_id) AS unique_members
FROM core.evo_entries
WHERE entry_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1
ORDER BY 1;
```

### Totalpass/Gympass Members
```sql
SELECT 
    CASE 
        WHEN gympass_id IS NOT NULL THEN 'Gympass'
        WHEN code_totalpass IS NOT NULL THEN 'Totalpass'
        ELSE 'Direct'
    END AS acquisition_channel,
    COUNT(*) AS members
FROM core.evo_members
WHERE status = 'Active'
GROUP BY 1;
```
