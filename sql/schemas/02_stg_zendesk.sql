-- =============================================================================
-- SKYFIT DATA LAKE - STAGING ZENDESK
-- =============================================================================
-- Tabelas para armazenar dados brutos do Zendesk em JSONB
-- Inclui rastreabilidade completa por run_id, blob e linha
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TICKETS RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_zendesk.tickets_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    -- Rastreabilidade
    source_blob_path TEXT NOT NULL,        -- Path completo no ADLS
    source_line_no INT NOT NULL,           -- Número da linha no arquivo
    run_id VARCHAR(100) NOT NULL,          -- ID do run de extração
    ingestion_date DATE NOT NULL,          -- Data de ingestão
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraint para evitar duplicatas
    UNIQUE (source_blob_path, source_line_no)
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_tickets_raw_run ON stg_zendesk.tickets_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_tickets_raw_date ON stg_zendesk.tickets_raw (ingestion_date);
CREATE INDEX IF NOT EXISTS idx_tickets_raw_ticket_id ON stg_zendesk.tickets_raw ((payload->>'id'));

COMMENT ON TABLE stg_zendesk.tickets_raw IS 'Tickets do Zendesk em formato bruto JSONB';

-- -----------------------------------------------------------------------------
-- USERS RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_zendesk.users_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_users_raw_run ON stg_zendesk.users_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_users_raw_date ON stg_zendesk.users_raw (ingestion_date);
CREATE INDEX IF NOT EXISTS idx_users_raw_user_id ON stg_zendesk.users_raw ((payload->>'id'));

COMMENT ON TABLE stg_zendesk.users_raw IS 'Usuários do Zendesk em formato bruto JSONB';

-- -----------------------------------------------------------------------------
-- ORGANIZATIONS RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_zendesk.organizations_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_orgs_raw_run ON stg_zendesk.organizations_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_orgs_raw_date ON stg_zendesk.organizations_raw (ingestion_date);
CREATE INDEX IF NOT EXISTS idx_orgs_raw_org_id ON stg_zendesk.organizations_raw ((payload->>'id'));

COMMENT ON TABLE stg_zendesk.organizations_raw IS 'Organizações do Zendesk em formato bruto JSONB';

-- -----------------------------------------------------------------------------
-- GROUPS RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_zendesk.groups_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_groups_raw_run ON stg_zendesk.groups_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_groups_raw_group_id ON stg_zendesk.groups_raw ((payload->>'id'));

COMMENT ON TABLE stg_zendesk.groups_raw IS 'Grupos do Zendesk em formato bruto JSONB';

-- -----------------------------------------------------------------------------
-- TICKET FIELDS RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_zendesk.ticket_fields_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_fields_raw_run ON stg_zendesk.ticket_fields_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_fields_raw_field_id ON stg_zendesk.ticket_fields_raw ((payload->>'id'));

COMMENT ON TABLE stg_zendesk.ticket_fields_raw IS 'Campos de ticket do Zendesk em formato bruto JSONB';

-- -----------------------------------------------------------------------------
-- TICKET FORMS RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_zendesk.ticket_forms_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_forms_raw_run ON stg_zendesk.ticket_forms_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_forms_raw_form_id ON stg_zendesk.ticket_forms_raw ((payload->>'id'));

COMMENT ON TABLE stg_zendesk.ticket_forms_raw IS 'Formulários de ticket do Zendesk em formato bruto JSONB';

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================
SELECT 
    table_schema,
    table_name,
    pg_catalog.obj_description(
        (quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass, 
        'pg_class'
    ) as description
FROM information_schema.tables
WHERE table_schema = 'stg_zendesk'
ORDER BY table_name;
