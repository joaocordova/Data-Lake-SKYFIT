-- =============================================================================
-- SKYFIT DATA LAKE - STAGING PIPEDRIVE
-- =============================================================================
-- Tabelas para armazenar dados brutos do Pipedrive em JSONB
-- Inclui rastreabilidade completa por run_id, blob e linha
-- Nota: Pipedrive tem 2 scopes (comercial e expansao)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DEALS RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_pipedrive.deals_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    -- Rastreabilidade
    scope VARCHAR(50) NOT NULL,            -- comercial ou expansao
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (scope, source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_deals_raw_run ON stg_pipedrive.deals_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_deals_raw_scope ON stg_pipedrive.deals_raw (scope);
CREATE INDEX IF NOT EXISTS idx_deals_raw_date ON stg_pipedrive.deals_raw (ingestion_date);
CREATE INDEX IF NOT EXISTS idx_deals_raw_deal_id ON stg_pipedrive.deals_raw ((payload->>'id'));

COMMENT ON TABLE stg_pipedrive.deals_raw IS 'Deals do Pipedrive em formato bruto JSONB';

-- -----------------------------------------------------------------------------
-- PERSONS RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_pipedrive.persons_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    scope VARCHAR(50) NOT NULL,
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (scope, source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_persons_raw_run ON stg_pipedrive.persons_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_persons_raw_scope ON stg_pipedrive.persons_raw (scope);
CREATE INDEX IF NOT EXISTS idx_persons_raw_person_id ON stg_pipedrive.persons_raw ((payload->>'id'));

COMMENT ON TABLE stg_pipedrive.persons_raw IS 'Pessoas do Pipedrive em formato bruto JSONB';

-- -----------------------------------------------------------------------------
-- ORGANIZATIONS RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_pipedrive.organizations_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    scope VARCHAR(50) NOT NULL,
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (scope, source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_pd_orgs_raw_run ON stg_pipedrive.organizations_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_pd_orgs_raw_scope ON stg_pipedrive.organizations_raw (scope);
CREATE INDEX IF NOT EXISTS idx_pd_orgs_raw_org_id ON stg_pipedrive.organizations_raw ((payload->>'id'));

COMMENT ON TABLE stg_pipedrive.organizations_raw IS 'Organizações do Pipedrive em formato bruto JSONB';

-- -----------------------------------------------------------------------------
-- ACTIVITIES RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_pipedrive.activities_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    scope VARCHAR(50) NOT NULL,
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (scope, source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_activities_raw_run ON stg_pipedrive.activities_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_activities_raw_scope ON stg_pipedrive.activities_raw (scope);
CREATE INDEX IF NOT EXISTS idx_activities_raw_act_id ON stg_pipedrive.activities_raw ((payload->>'id'));

COMMENT ON TABLE stg_pipedrive.activities_raw IS 'Atividades do Pipedrive em formato bruto JSONB';

-- -----------------------------------------------------------------------------
-- PIPELINES RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_pipedrive.pipelines_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    scope VARCHAR(50) NOT NULL,
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (scope, source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_pipelines_raw_run ON stg_pipedrive.pipelines_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_raw_pipeline_id ON stg_pipedrive.pipelines_raw ((payload->>'id'));

COMMENT ON TABLE stg_pipedrive.pipelines_raw IS 'Pipelines do Pipedrive em formato bruto JSONB';

-- -----------------------------------------------------------------------------
-- STAGES RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_pipedrive.stages_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    scope VARCHAR(50) NOT NULL,
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (scope, source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_stages_raw_run ON stg_pipedrive.stages_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_stages_raw_stage_id ON stg_pipedrive.stages_raw ((payload->>'id'));

COMMENT ON TABLE stg_pipedrive.stages_raw IS 'Estágios do Pipedrive em formato bruto JSONB';

-- -----------------------------------------------------------------------------
-- USERS RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_pipedrive.users_raw (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    
    scope VARCHAR(50) NOT NULL,
    source_blob_path TEXT NOT NULL,
    source_line_no INT NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    ingestion_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (scope, source_blob_path, source_line_no)
);

CREATE INDEX IF NOT EXISTS idx_pd_users_raw_run ON stg_pipedrive.users_raw (run_id);
CREATE INDEX IF NOT EXISTS idx_pd_users_raw_user_id ON stg_pipedrive.users_raw ((payload->>'id'));

COMMENT ON TABLE stg_pipedrive.users_raw IS 'Usuários do Pipedrive em formato bruto JSONB';

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
WHERE table_schema = 'stg_pipedrive'
ORDER BY table_name;
