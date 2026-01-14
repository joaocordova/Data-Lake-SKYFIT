-- =============================================================================
-- SKYFIT DATA LAKE - CORE PIPEDRIVE (Silver Layer)
-- =============================================================================
-- Tabelas normalizadas e tipadas para consumo analítico
-- Nota: Pipedrive tem múltiplos scopes (comercial, expansao)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- PIPELINES
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.pd_pipelines (
    pipeline_id BIGINT NOT NULL,
    scope VARCHAR(50) NOT NULL,            -- comercial ou expansao
    
    name TEXT NOT NULL,
    url_title TEXT,
    order_nr INT,
    active BOOLEAN DEFAULT TRUE,
    deal_probability BOOLEAN,
    add_time TIMESTAMPTZ,
    update_time TIMESTAMPTZ,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (pipeline_id, scope)
);

COMMENT ON TABLE core.pd_pipelines IS 'Pipelines do Pipedrive normalizados';

-- -----------------------------------------------------------------------------
-- STAGES
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.pd_stages (
    stage_id BIGINT NOT NULL,
    scope VARCHAR(50) NOT NULL,
    
    pipeline_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    order_nr INT,
    active_flag BOOLEAN DEFAULT TRUE,
    deal_probability INT,
    rotten_flag BOOLEAN,
    rotten_days INT,
    add_time TIMESTAMPTZ,
    update_time TIMESTAMPTZ,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (stage_id, scope)
);

CREATE INDEX IF NOT EXISTS idx_pd_stages_pipeline ON core.pd_stages (pipeline_id, scope);

COMMENT ON TABLE core.pd_stages IS 'Estágios dos pipelines do Pipedrive normalizados';

-- -----------------------------------------------------------------------------
-- USERS (Vendedores/Agentes)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.pd_users (
    user_id BIGINT NOT NULL,
    scope VARCHAR(50) NOT NULL,
    
    name TEXT,
    email TEXT,
    phone TEXT,
    active_flag BOOLEAN,
    is_admin BOOLEAN,
    role_id BIGINT,
    icon_url TEXT,
    timezone_name TEXT,
    timezone_offset TEXT,
    locale TEXT,
    default_currency TEXT,
    created TIMESTAMPTZ,
    modified TIMESTAMPTZ,
    last_login TIMESTAMPTZ,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (user_id, scope)
);

CREATE INDEX IF NOT EXISTS idx_pd_users_email ON core.pd_users (email);

COMMENT ON TABLE core.pd_users IS 'Usuários do Pipedrive normalizados';

-- -----------------------------------------------------------------------------
-- ORGANIZATIONS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.pd_organizations (
    org_id BIGINT NOT NULL,
    scope VARCHAR(50) NOT NULL,
    
    name TEXT,
    owner_id BIGINT,
    address TEXT,
    address_subpremise TEXT,
    address_street_number TEXT,
    address_route TEXT,
    address_sublocality TEXT,
    address_locality TEXT,
    address_admin_area_level_1 TEXT,
    address_admin_area_level_2 TEXT,
    address_country TEXT,
    address_postal_code TEXT,
    cc_email TEXT,
    active_flag BOOLEAN DEFAULT TRUE,
    
    -- Contadores
    people_count INT,
    activities_count INT,
    done_activities_count INT,
    undone_activities_count INT,
    files_count INT,
    notes_count INT,
    followers_count INT,
    won_deals_count INT,
    lost_deals_count INT,
    open_deals_count INT,
    related_open_deals_count INT,
    related_closed_deals_count INT,
    related_won_deals_count INT,
    related_lost_deals_count INT,
    
    next_activity_date DATE,
    next_activity_time TIME,
    next_activity_id BIGINT,
    last_activity_id BIGINT,
    last_activity_date DATE,
    
    add_time TIMESTAMPTZ,
    update_time TIMESTAMPTZ,
    
    -- Custom fields em JSONB para flexibilidade
    custom_fields JSONB,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (org_id, scope)
);

CREATE INDEX IF NOT EXISTS idx_pd_orgs_name ON core.pd_organizations (name);
CREATE INDEX IF NOT EXISTS idx_pd_orgs_owner ON core.pd_organizations (owner_id, scope);

COMMENT ON TABLE core.pd_organizations IS 'Organizações do Pipedrive normalizadas';

-- -----------------------------------------------------------------------------
-- PERSONS (Contatos)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.pd_persons (
    person_id BIGINT NOT NULL,
    scope VARCHAR(50) NOT NULL,
    
    name TEXT,
    first_name TEXT,
    last_name TEXT,
    owner_id BIGINT,
    org_id BIGINT,
    
    -- Contatos (pode haver múltiplos, mantemos o principal + JSONB)
    primary_email TEXT,
    primary_phone TEXT,
    emails JSONB,                          -- Array de emails
    phones JSONB,                          -- Array de phones
    
    active_flag BOOLEAN DEFAULT TRUE,
    label BIGINT,
    
    -- Contadores
    open_deals_count INT,
    related_open_deals_count INT,
    closed_deals_count INT,
    related_closed_deals_count INT,
    participant_open_deals_count INT,
    participant_closed_deals_count INT,
    activities_count INT,
    done_activities_count INT,
    undone_activities_count INT,
    files_count INT,
    notes_count INT,
    followers_count INT,
    won_deals_count INT,
    related_won_deals_count INT,
    lost_deals_count INT,
    related_lost_deals_count INT,
    
    next_activity_date DATE,
    next_activity_time TIME,
    next_activity_id BIGINT,
    last_activity_id BIGINT,
    last_activity_date DATE,
    
    add_time TIMESTAMPTZ,
    update_time TIMESTAMPTZ,
    
    custom_fields JSONB,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (person_id, scope)
);

CREATE INDEX IF NOT EXISTS idx_pd_persons_org ON core.pd_persons (org_id, scope);
CREATE INDEX IF NOT EXISTS idx_pd_persons_owner ON core.pd_persons (owner_id, scope);
CREATE INDEX IF NOT EXISTS idx_pd_persons_email ON core.pd_persons (primary_email);

COMMENT ON TABLE core.pd_persons IS 'Pessoas/Contatos do Pipedrive normalizados';

-- -----------------------------------------------------------------------------
-- DEALS (Negócios)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.pd_deals (
    deal_id BIGINT NOT NULL,
    scope VARCHAR(50) NOT NULL,
    
    title TEXT,
    status VARCHAR(20),                    -- open, won, lost
    
    -- Valores monetários
    value NUMERIC(18,2),
    currency VARCHAR(10),
    weighted_value NUMERIC(18,2),
    weighted_value_currency VARCHAR(10),
    
    -- Relacionamentos
    person_id BIGINT,
    org_id BIGINT,
    user_id BIGINT,                        -- owner
    pipeline_id BIGINT,
    stage_id BIGINT,
    
    -- Datas importantes
    add_time TIMESTAMPTZ,
    update_time TIMESTAMPTZ,
    stage_change_time TIMESTAMPTZ,
    expected_close_date DATE,
    close_time TIMESTAMPTZ,
    won_time TIMESTAMPTZ,
    lost_time TIMESTAMPTZ,
    first_won_time TIMESTAMPTZ,
    
    -- Motivo de perda
    lost_reason TEXT,
    
    -- Flags
    active BOOLEAN DEFAULT TRUE,
    deleted BOOLEAN DEFAULT FALSE,
    
    -- Probabilidade e idade
    probability NUMERIC(5,2),
    stage_order_nr INT,
    rotten_time TIMESTAMPTZ,
    
    -- Contadores
    activities_count INT,
    done_activities_count INT,
    undone_activities_count INT,
    participants_count INT,
    files_count INT,
    notes_count INT,
    followers_count INT,
    email_messages_count INT,
    
    -- Próxima atividade
    next_activity_id BIGINT,
    next_activity_subject TEXT,
    next_activity_type TEXT,
    next_activity_duration TIME,
    next_activity_note TEXT,
    next_activity_time TIME,
    next_activity_date DATE,
    
    -- Última atividade
    last_activity_id BIGINT,
    last_activity_date DATE,
    
    -- Origem
    origin TEXT,
    origin_id TEXT,
    channel BIGINT,
    channel_id TEXT,
    
    -- Custom fields
    custom_fields JSONB,
    
    -- Metadados
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (deal_id, scope)
);

-- Índices para queries comuns
CREATE INDEX IF NOT EXISTS idx_pd_deals_status ON core.pd_deals (status, scope);
CREATE INDEX IF NOT EXISTS idx_pd_deals_pipeline ON core.pd_deals (pipeline_id, scope);
CREATE INDEX IF NOT EXISTS idx_pd_deals_stage ON core.pd_deals (stage_id, scope);
CREATE INDEX IF NOT EXISTS idx_pd_deals_person ON core.pd_deals (person_id, scope);
CREATE INDEX IF NOT EXISTS idx_pd_deals_org ON core.pd_deals (org_id, scope);
CREATE INDEX IF NOT EXISTS idx_pd_deals_user ON core.pd_deals (user_id, scope);
CREATE INDEX IF NOT EXISTS idx_pd_deals_add_time ON core.pd_deals (add_time);
CREATE INDEX IF NOT EXISTS idx_pd_deals_close_time ON core.pd_deals (close_time);
CREATE INDEX IF NOT EXISTS idx_pd_deals_won_time ON core.pd_deals (won_time);
CREATE INDEX IF NOT EXISTS idx_pd_deals_expected_close ON core.pd_deals (expected_close_date);

COMMENT ON TABLE core.pd_deals IS 'Deals/Negócios do Pipedrive normalizados';

-- -----------------------------------------------------------------------------
-- ACTIVITIES
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.pd_activities (
    activity_id BIGINT NOT NULL,
    scope VARCHAR(50) NOT NULL,
    
    type VARCHAR(50),                      -- call, meeting, task, deadline, email, lunch
    subject TEXT,
    note TEXT,
    
    -- Status
    done BOOLEAN,
    busy_flag BOOLEAN,
    
    -- Relacionamentos
    user_id BIGINT,
    deal_id BIGINT,
    person_id BIGINT,
    org_id BIGINT,
    lead_id TEXT,
    project_id BIGINT,
    
    -- Datas e horários
    due_date DATE,
    due_time TIME,
    duration TIME,
    add_time TIMESTAMPTZ,
    marked_as_done_time TIMESTAMPTZ,
    update_time TIMESTAMPTZ,
    
    -- Localização
    location TEXT,
    location_subpremise TEXT,
    location_street_number TEXT,
    location_route TEXT,
    location_sublocality TEXT,
    location_locality TEXT,
    location_admin_area_level_1 TEXT,
    location_admin_area_level_2 TEXT,
    location_country TEXT,
    location_postal_code TEXT,
    location_formatted_address TEXT,
    
    -- Participantes
    participants JSONB,
    attendees JSONB,
    
    -- Conference link
    conference_meeting_client TEXT,
    conference_meeting_url TEXT,
    conference_meeting_id TEXT,
    
    -- Flags
    public_description TEXT,
    active_flag BOOLEAN,
    
    -- Metadados
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (activity_id, scope)
);

CREATE INDEX IF NOT EXISTS idx_pd_activities_deal ON core.pd_activities (deal_id, scope);
CREATE INDEX IF NOT EXISTS idx_pd_activities_person ON core.pd_activities (person_id, scope);
CREATE INDEX IF NOT EXISTS idx_pd_activities_user ON core.pd_activities (user_id, scope);
CREATE INDEX IF NOT EXISTS idx_pd_activities_due_date ON core.pd_activities (due_date);
CREATE INDEX IF NOT EXISTS idx_pd_activities_type ON core.pd_activities (type);

COMMENT ON TABLE core.pd_activities IS 'Atividades do Pipedrive normalizadas';

-- =============================================================================
-- VIEWS ÚTEIS PARA ANÁLISE
-- =============================================================================

-- View de funil de vendas
CREATE OR REPLACE VIEW core.vw_pd_sales_funnel AS
SELECT 
    d.scope,
    p.name AS pipeline_name,
    s.name AS stage_name,
    s.order_nr AS stage_order,
    d.status,
    COUNT(*) AS deal_count,
    SUM(d.value) AS total_value,
    AVG(d.value) AS avg_value,
    d.currency
FROM core.pd_deals d
JOIN core.pd_pipelines p ON d.pipeline_id = p.pipeline_id AND d.scope = p.scope
JOIN core.pd_stages s ON d.stage_id = s.stage_id AND d.scope = s.scope
WHERE d.active = TRUE
GROUP BY d.scope, p.name, s.name, s.order_nr, d.status, d.currency
ORDER BY d.scope, p.name, s.order_nr;

COMMENT ON VIEW core.vw_pd_sales_funnel IS 'View de funil de vendas por pipeline e estágio';

-- View de performance por vendedor
CREATE OR REPLACE VIEW core.vw_pd_seller_performance AS
SELECT 
    d.scope,
    u.name AS seller_name,
    u.email AS seller_email,
    COUNT(*) FILTER (WHERE d.status = 'open') AS open_deals,
    COUNT(*) FILTER (WHERE d.status = 'won') AS won_deals,
    COUNT(*) FILTER (WHERE d.status = 'lost') AS lost_deals,
    SUM(d.value) FILTER (WHERE d.status = 'won') AS won_value,
    SUM(d.value) FILTER (WHERE d.status = 'open') AS pipeline_value,
    d.currency,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE d.status = 'won') / 
        NULLIF(COUNT(*) FILTER (WHERE d.status IN ('won', 'lost')), 0),
        2
    ) AS win_rate_pct
FROM core.pd_deals d
JOIN core.pd_users u ON d.user_id = u.user_id AND d.scope = u.scope
GROUP BY d.scope, u.name, u.email, d.currency;

COMMENT ON VIEW core.vw_pd_seller_performance IS 'View de performance de vendas por vendedor';

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================
SELECT 
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'core' 
  AND table_name LIKE 'pd_%'
ORDER BY table_type, table_name;
