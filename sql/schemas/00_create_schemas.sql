-- =============================================================================
-- SKYFIT DATA LAKE - CRIAÇÃO DE SCHEMAS
-- =============================================================================
-- Executar como primeiro passo na configuração do banco
-- =============================================================================

-- Schema para dados brutos (staging) do Pipedrive
CREATE SCHEMA IF NOT EXISTS stg_pipedrive;
COMMENT ON SCHEMA stg_pipedrive IS 'Staging: dados brutos do Pipedrive em JSONB';

-- Schema para dados brutos (staging) do Zendesk
CREATE SCHEMA IF NOT EXISTS stg_zendesk;
COMMENT ON SCHEMA stg_zendesk IS 'Staging: dados brutos do Zendesk em JSONB';

-- Schema para dados brutos (staging) do EVO (futuro)
CREATE SCHEMA IF NOT EXISTS stg_evo;
COMMENT ON SCHEMA stg_evo IS 'Staging: dados brutos do EVO em JSONB';

-- Schema para dados normalizados (core/silver)
CREATE SCHEMA IF NOT EXISTS core;
COMMENT ON SCHEMA core IS 'Core: dados normalizados e tipados (Silver Layer)';

-- Schema para dados analíticos (gold/marts)
CREATE SCHEMA IF NOT EXISTS analytics;
COMMENT ON SCHEMA analytics IS 'Analytics: views e tabelas agregadas para BI (Gold Layer)';

-- Schema para controle e metadados
CREATE SCHEMA IF NOT EXISTS meta;
COMMENT ON SCHEMA meta IS 'Metadados: controle de execuções, watermarks, logs';

-- =============================================================================
-- TABELA DE CONTROLE DE RUNS
-- =============================================================================
CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,           -- pipedrive, zendesk, evo
    scope VARCHAR(50),                     -- comercial, expansao, support
    entity VARCHAR(100) NOT NULL,          -- deals, tickets, etc
    run_id VARCHAR(100) NOT NULL,          -- ID único do run
    ingestion_date DATE NOT NULL,
    stage VARCHAR(20) NOT NULL,            -- bronze, stg, core
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status VARCHAR(20) DEFAULT 'RUNNING',  -- RUNNING, SUCCESS, FAILED
    records_processed INT DEFAULT 0,
    error_message TEXT,
    metadata JSONB,
    
    UNIQUE (source, scope, entity, run_id, stage)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_lookup 
ON meta.pipeline_runs (source, entity, run_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date 
ON meta.pipeline_runs (ingestion_date DESC);

-- =============================================================================
-- GRANTS (ajustar conforme necessidade)
-- =============================================================================
-- Exemplo: se tiver um user específico para o Power BI
-- GRANT USAGE ON SCHEMA analytics TO powerbi_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO powerbi_user;

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================
SELECT 
    schema_name,
    pg_catalog.obj_description(oid, 'pg_namespace') as description
FROM information_schema.schemata s
JOIN pg_catalog.pg_namespace n ON n.nspname = s.schema_name
WHERE schema_name IN ('stg_pipedrive', 'stg_zendesk', 'stg_evo', 'core', 'analytics', 'meta')
ORDER BY schema_name;
