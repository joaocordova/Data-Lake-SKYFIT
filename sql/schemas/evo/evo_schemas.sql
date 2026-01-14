-- ============================================================================
-- SKYFIT DATA LAKE - EVO SCHEMAS v5
-- ============================================================================
-- Versão: 5.0
-- Data: 2026-01-11
-- 
-- Este script:
-- 1. CRIA tabelas se não existirem
-- 2. ADICIONA colunas faltantes se tabelas existirem
-- ============================================================================

-- ============================================================================
-- SCHEMAS
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS stg_evo;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================================================
-- STG TABLES - JSONB RAW
-- ============================================================================

-- PROSPECTS RAW
CREATE TABLE IF NOT EXISTS stg_evo.prospects_raw (
    prospect_id BIGINT PRIMARY KEY,
    raw_data JSONB NOT NULL,
    _source_file TEXT,
    run_id TEXT,
    ingestion_date DATE,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- SALES RAW
CREATE TABLE IF NOT EXISTS stg_evo.sales_raw (
    sale_id BIGINT PRIMARY KEY,
    raw_data JSONB NOT NULL,
    _source_file TEXT,
    run_id TEXT,
    ingestion_date DATE,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- MEMBERS RAW
CREATE TABLE IF NOT EXISTS stg_evo.members_raw (
    member_id BIGINT PRIMARY KEY,
    raw_data JSONB NOT NULL,
    _source_file TEXT,
    run_id TEXT,
    ingestion_date DATE,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- ADICIONA COLUNAS FALTANTES (se tabelas já existiam)
-- ============================================================================

-- SALES
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'stg_evo' AND table_name = 'sales_raw' AND column_name = 'run_id') THEN
        ALTER TABLE stg_evo.sales_raw ADD COLUMN run_id TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'stg_evo' AND table_name = 'sales_raw' AND column_name = 'ingestion_date') THEN
        ALTER TABLE stg_evo.sales_raw ADD COLUMN ingestion_date DATE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'stg_evo' AND table_name = 'sales_raw' AND column_name = '_updated_at') THEN
        ALTER TABLE stg_evo.sales_raw ADD COLUMN _updated_at TIMESTAMPTZ DEFAULT NOW();
    END IF;
END $$;

-- MEMBERS
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'stg_evo' AND table_name = 'members_raw' AND column_name = 'run_id') THEN
        ALTER TABLE stg_evo.members_raw ADD COLUMN run_id TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'stg_evo' AND table_name = 'members_raw' AND column_name = 'ingestion_date') THEN
        ALTER TABLE stg_evo.members_raw ADD COLUMN ingestion_date DATE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'stg_evo' AND table_name = 'members_raw' AND column_name = '_updated_at') THEN
        ALTER TABLE stg_evo.members_raw ADD COLUMN _updated_at TIMESTAMPTZ DEFAULT NOW();
    END IF;
END $$;

-- PROSPECTS
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'stg_evo' AND table_name = 'prospects_raw' AND column_name = 'run_id') THEN
        ALTER TABLE stg_evo.prospects_raw ADD COLUMN run_id TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'stg_evo' AND table_name = 'prospects_raw' AND column_name = 'ingestion_date') THEN
        ALTER TABLE stg_evo.prospects_raw ADD COLUMN ingestion_date DATE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'stg_evo' AND table_name = 'prospects_raw' AND column_name = '_updated_at') THEN
        ALTER TABLE stg_evo.prospects_raw ADD COLUMN _updated_at TIMESTAMPTZ DEFAULT NOW();
    END IF;
END $$;

-- ============================================================================
-- ENTRIES STG (PARTICIONADA POR ANO)
-- ============================================================================
CREATE TABLE IF NOT EXISTS stg_evo.entries_raw (
    entry_id BIGINT NOT NULL,
    entry_date TIMESTAMPTZ NOT NULL,
    raw_data JSONB NOT NULL,
    _source_file TEXT,
    run_id TEXT,
    ingestion_date DATE,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entry_id, entry_date)
) PARTITION BY RANGE (entry_date);

-- Partições por ano (ignora se já existem)
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 2020..2026 LOOP
        BEGIN
            EXECUTE format(
                'CREATE TABLE stg_evo.entries_raw_%s PARTITION OF stg_evo.entries_raw
                 FOR VALUES FROM (%L) TO (%L)',
                y,
                y || '-01-01',
                (y + 1) || '-01-01'
            );
        EXCEPTION WHEN duplicate_table THEN
            NULL; -- Ignora se já existe
        END;
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS idx_stg_entries_date ON stg_evo.entries_raw(entry_date);
CREATE INDEX IF NOT EXISTS idx_stg_entries_run ON stg_evo.entries_raw(run_id);

-- ============================================================================
-- CORE TABLES
-- ============================================================================

-- PROSPECTS CORE
CREATE TABLE IF NOT EXISTS core.evo_prospects (
    prospect_id BIGINT PRIMARY KEY,
    branch_id BIGINT,
    branch_name TEXT,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT GENERATED ALWAYS AS (TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))) STORED,
    document TEXT,
    email TEXT,
    cellphone TEXT,
    gender TEXT,
    birth_date DATE,
    address TEXT,
    address_number TEXT,
    complement TEXT,
    neighborhood TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    zip_code TEXT,
    signup_type TEXT,
    mkt_channel TEXT,
    current_step TEXT,
    gympass_id TEXT,
    conversion_date TIMESTAMPTZ,
    member_id BIGINT,
    is_converted BOOLEAN GENERATED ALWAYS AS (member_id IS NOT NULL) STORED,
    responsible_name TEXT,
    responsible_document TEXT,
    responsible_is_financial BOOLEAN,
    register_date TIMESTAMPTZ NOT NULL,
    custom_fields JSONB,
    _source_run_id TEXT,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- SALES CORE
CREATE TABLE IF NOT EXISTS core.evo_sales (
    sale_id BIGINT PRIMARY KEY,
    member_id BIGINT,
    prospect_id BIGINT,
    employee_id BIGINT,
    employee_sale_id BIGINT,
    employee_sale_name TEXT,
    personal_id BIGINT,
    branch_id BIGINT,
    sale_date TIMESTAMPTZ,
    sale_date_server TIMESTAMPTZ,
    update_date TIMESTAMPTZ,
    sale_source INT,
    observations TEXT,
    corporate_partnership_id BIGINT,
    corporate_partnership_name TEXT,
    sale_recurrency_id BIGINT,
    removed BOOLEAN DEFAULT FALSE,
    removal_date TIMESTAMPTZ,
    employee_removal_id BIGINT,
    sale_migration_id TEXT,
    cart_token TEXT,
    _source_run_id TEXT,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_core_sales_member ON core.evo_sales(member_id);
CREATE INDEX IF NOT EXISTS idx_core_sales_date ON core.evo_sales(sale_date);

-- SALE ITEMS CORE
CREATE TABLE IF NOT EXISTS core.evo_sale_items (
    sale_item_id BIGINT PRIMARY KEY,
    sale_id BIGINT NOT NULL,
    description TEXT,
    item TEXT,
    item_value DECIMAL(15,2),
    sale_value DECIMAL(15,2),
    sale_value_without_credit DECIMAL(15,2),
    quantity INT,
    discount DECIMAL(15,2),
    corporate_discount DECIMAL(15,2),
    tax DECIMAL(15,2),
    value_next_month DECIMAL(15,2),
    membership_id BIGINT,
    membership_renewed_id BIGINT,
    member_membership_id BIGINT,
    product_id BIGINT,
    service_id BIGINT,
    corporate_partnership_id BIGINT,
    corporate_partnership_name TEXT,
    membership_start_date TIMESTAMPTZ,
    num_members INT,
    voucher TEXT,
    accounting_code TEXT,
    municipal_service_code TEXT,
    fl_receipt_only BOOLEAN,
    fl_swimming BOOLEAN,
    fl_allow_locker BOOLEAN,
    sale_item_migration_id TEXT,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_core_sale_items_sale ON core.evo_sale_items(sale_id);

-- RECEIVABLES CORE
CREATE TABLE IF NOT EXISTS core.evo_receivables (
    receivable_id BIGINT PRIMARY KEY,
    sale_id BIGINT NOT NULL,
    registration_date TIMESTAMPTZ,
    due_date TIMESTAMPTZ,
    receiving_date TIMESTAMPTZ,
    cancellation_date TIMESTAMPTZ,
    update_date TIMESTAMPTZ,
    amount DECIMAL(15,2),
    amount_paid DECIMAL(15,2),
    status_id INT,
    status_name TEXT,
    current_installment INT,
    total_installments INT,
    payment_type_id INT,
    payment_type_name TEXT,
    authorization TEXT,
    tid TEXT,
    nsu TEXT,
    card_flag TEXT,
    transaction_token TEXT,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_core_receivables_sale ON core.evo_receivables(sale_id);
CREATE INDEX IF NOT EXISTS idx_core_receivables_due ON core.evo_receivables(due_date);

-- MEMBERS CORE
CREATE TABLE IF NOT EXISTS core.evo_members (
    member_id BIGINT PRIMARY KEY,
    branch_id BIGINT,
    branch_name TEXT,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT GENERATED ALWAYS AS (TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))) STORED,
    register_name TEXT,
    register_last_name TEXT,
    use_preferred_name BOOLEAN DEFAULT FALSE,
    document TEXT,
    document_id TEXT,
    email TEXT,
    cellphone TEXT,
    gender TEXT,
    birth_date DATE,
    marital_status TEXT,
    address TEXT,
    address_number TEXT,
    complement TEXT,
    neighborhood TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    zip_code TEXT,
    access_card_number TEXT,
    access_blocked BOOLEAN DEFAULT FALSE,
    blocked_reason TEXT,
    status TEXT,
    membership_status TEXT,
    penalized BOOLEAN DEFAULT FALSE,
    total_fit_coins DECIMAL(15,2),
    register_date TIMESTAMPTZ,
    conversion_date TIMESTAMPTZ,
    last_access_date TIMESTAMPTZ,
    update_date TIMESTAMPTZ,
    photo_url TEXT,
    -- Integrações com parceiros
    gympass_id TEXT,
    code_totalpass TEXT,
    user_id_gurupass TEXT,
    client_with_promotional_restriction BOOLEAN DEFAULT FALSE,
    -- Personal trainer
    personal_trainer BOOLEAN DEFAULT FALSE,
    personal_type TEXT,
    cref TEXT,
    cref_expiration_date DATE,
    -- Funcionários vinculados
    employee_consultant_id BIGINT,
    employee_consultant_name TEXT,
    employee_instructor_id BIGINT,
    employee_instructor_name TEXT,
    employee_personal_id BIGINT,
    employee_personal_name TEXT,
    member_migration_id TEXT,
    _source_run_id TEXT,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_core_members_branch ON core.evo_members(branch_id);
CREATE INDEX IF NOT EXISTS idx_core_members_status ON core.evo_members(status);

-- MEMBER MEMBERSHIPS CORE
CREATE TABLE IF NOT EXISTS core.evo_member_memberships (
    id SERIAL PRIMARY KEY,
    member_membership_id BIGINT NOT NULL,
    member_id BIGINT NOT NULL,
    membership_id BIGINT,
    membership_name TEXT,
    membership_renewed_id BIGINT,
    sale_id BIGINT,
    sale_date TIMESTAMPTZ,
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    cancel_date TIMESTAMPTZ,
    cancel_date_on TIMESTAMPTZ,
    cancel_creation_date TIMESTAMPTZ,
    membership_status TEXT,
    value_next_month DECIMAL(15,2),
    original_value DECIMAL(15,2),
    next_charge TIMESTAMPTZ,
    next_date_suspension TIMESTAMPTZ,
    category_membership_id BIGINT,
    loyalty_end_date TIMESTAMPTZ,
    assessment_end_date TIMESTAMPTZ,
    acceptance_date TIMESTAMPTZ,
    num_members INT,
    fl_allow_locker BOOLEAN,
    fl_additional_membership BOOLEAN,
    allow_les_mills BOOLEAN,
    allows_cancellation_by_app BOOLEAN,
    signed_terms BOOLEAN,
    limitless BOOLEAN,
    weekly_limit INT,
    bioimpedance_amount INT,
    concluded_sessions INT,
    pending_sessions INT,
    scheduled_sessions INT,
    pending_repositions INT,
    repositions_total INT,
    bonus_sessions INT,
    number_suspension_times INT,
    max_suspension_days INT,
    minimum_suspension_days INT,
    disponible_suspension_days INT,
    disponible_suspension_times INT,
    days_left_to_freeze INT,
    contract_printing TEXT,
    freezes JSONB,
    sessions JSONB,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(member_id, member_membership_id)
);

-- MEMBER CONTACTS CORE
CREATE TABLE IF NOT EXISTS core.evo_member_contacts (
    id SERIAL PRIMARY KEY,
    phone_id BIGINT,
    member_id BIGINT NOT NULL,
    contact_type_id INT,
    contact_type TEXT,
    ddi TEXT,
    description TEXT,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(member_id, phone_id)
);

-- ENTRIES CORE (PARTICIONADA)
CREATE TABLE IF NOT EXISTS core.evo_entries (
    entry_id BIGINT NOT NULL,
    entry_date TIMESTAMPTZ NOT NULL,
    entry_date_turn TIMESTAMPTZ,
    timezone TEXT,
    member_id BIGINT,
    member_name TEXT,
    prospect_id BIGINT,
    prospect_name TEXT,
    employee_id BIGINT,
    employee_name TEXT,
    branch_id BIGINT,
    entry_type TEXT,
    entry_action TEXT,
    device TEXT,
    block_reason TEXT,
    releases_by_id BIGINT,
    migration_id TEXT,
    _source_run_id TEXT,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entry_id, entry_date)
) PARTITION BY RANGE (entry_date);

-- Partições CORE por ano
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 2020..2026 LOOP
        BEGIN
            EXECUTE format(
                'CREATE TABLE core.evo_entries_%s PARTITION OF core.evo_entries
                 FOR VALUES FROM (%L) TO (%L)',
                y,
                y || '-01-01',
                (y + 1) || '-01-01'
            );
        EXCEPTION WHEN duplicate_table THEN
            NULL;
        END;
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS idx_core_entries_date ON core.evo_entries(entry_date);
CREATE INDEX IF NOT EXISTS idx_core_entries_member ON core.evo_entries(member_id) WHERE member_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_entries_branch ON core.evo_entries(branch_id);

-- ============================================================================
-- VERIFICAÇÃO FINAL
-- ============================================================================
SELECT 
    schemaname || '.' || tablename AS tabela,
    CASE 
        WHEN tablename LIKE '%_raw%' THEN 'STG' 
        ELSE 'CORE' 
    END AS camada
FROM pg_tables 
WHERE schemaname IN ('stg_evo', 'core')
  AND tablename NOT LIKE '%_20%'  -- Exclui partições
ORDER BY schemaname, tablename;

SELECT '✅ Schema EVO v5 aplicado com sucesso!' AS status;
