-- =============================================================================
-- SKYFIT DATA LAKE - CORE ZENDESK (Silver Layer)
-- =============================================================================
-- Tabelas normalizadas e tipadas para consumo analítico
-- =============================================================================

-- -----------------------------------------------------------------------------
-- ORGANIZATIONS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.zd_organizations (
    organization_id BIGINT PRIMARY KEY,
    name TEXT,
    domain_names JSONB,                    -- Array de domínios
    details TEXT,
    notes TEXT,
    group_id BIGINT,
    shared_tickets BOOLEAN,
    shared_comments BOOLEAN,
    tags JSONB,                            -- Array de tags
    organization_fields JSONB,             -- Custom fields
    external_id TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    
    -- Metadados de carga
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_zd_orgs_name ON core.zd_organizations (name);
CREATE INDEX IF NOT EXISTS idx_zd_orgs_updated ON core.zd_organizations (updated_at);

COMMENT ON TABLE core.zd_organizations IS 'Organizações do Zendesk normalizadas';

-- -----------------------------------------------------------------------------
-- USERS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.zd_users (
    user_id BIGINT PRIMARY KEY,
    name TEXT,
    email TEXT,
    phone TEXT,
    role VARCHAR(50),                      -- end-user, agent, admin
    organization_id BIGINT,
    time_zone TEXT,
    locale TEXT,
    active BOOLEAN,
    verified BOOLEAN,
    suspended BOOLEAN,
    tags JSONB,
    user_fields JSONB,
    external_id TEXT,
    alias TEXT,
    notes TEXT,
    details TEXT,
    default_group_id BIGINT,
    only_private_comments BOOLEAN,
    restricted_agent BOOLEAN,
    shared BOOLEAN,
    shared_agent BOOLEAN,
    signature TEXT,
    ticket_restriction VARCHAR(50),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    last_login_at TIMESTAMPTZ,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_zd_users_email ON core.zd_users (email);
CREATE INDEX IF NOT EXISTS idx_zd_users_org ON core.zd_users (organization_id);
CREATE INDEX IF NOT EXISTS idx_zd_users_role ON core.zd_users (role);
CREATE INDEX IF NOT EXISTS idx_zd_users_updated ON core.zd_users (updated_at);

COMMENT ON TABLE core.zd_users IS 'Usuários do Zendesk normalizados';

-- -----------------------------------------------------------------------------
-- GROUPS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.zd_groups (
    group_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    is_public BOOLEAN,
    deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE core.zd_groups IS 'Grupos do Zendesk normalizados';

-- -----------------------------------------------------------------------------
-- TICKET FIELDS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.zd_ticket_fields (
    field_id BIGINT PRIMARY KEY,
    title TEXT,
    type VARCHAR(50),
    description TEXT,
    position INT,
    active BOOLEAN,
    required BOOLEAN,
    collapsed_for_agents BOOLEAN,
    regexp_for_validation TEXT,
    title_in_portal TEXT,
    visible_in_portal BOOLEAN,
    editable_in_portal BOOLEAN,
    required_in_portal BOOLEAN,
    tag TEXT,
    custom_field_options JSONB,            -- Opções para dropdowns
    system_field_options JSONB,
    removable BOOLEAN,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE core.zd_ticket_fields IS 'Campos de ticket do Zendesk normalizados';

-- -----------------------------------------------------------------------------
-- TICKET FORMS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.zd_ticket_forms (
    form_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    display_name TEXT,
    position INT,
    active BOOLEAN,
    default_form BOOLEAN,
    end_user_visible BOOLEAN,
    in_all_brands BOOLEAN,
    raw_name TEXT,
    raw_display_name TEXT,
    ticket_field_ids JSONB,                -- Array de IDs dos campos
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE core.zd_ticket_forms IS 'Formulários de ticket do Zendesk normalizados';

-- -----------------------------------------------------------------------------
-- TICKETS (tabela principal)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.zd_tickets (
    ticket_id BIGINT PRIMARY KEY,
    
    -- Identificação
    external_id TEXT,
    subject TEXT,
    description TEXT,
    raw_subject TEXT,
    
    -- Status e prioridade
    status VARCHAR(50),                    -- new, open, pending, hold, solved, closed
    priority VARCHAR(20),                  -- low, normal, high, urgent
    type VARCHAR(20),                      -- problem, incident, question, task
    
    -- Relacionamentos principais
    requester_id BIGINT,
    submitter_id BIGINT,
    assignee_id BIGINT,
    organization_id BIGINT,
    group_id BIGINT,
    brand_id BIGINT,
    ticket_form_id BIGINT,
    
    -- Configurações
    is_public BOOLEAN,
    has_incidents BOOLEAN,
    allow_channelback BOOLEAN,
    allow_attachments BOOLEAN,
    
    -- SLA e satisfação
    satisfaction_rating JSONB,
    satisfaction_score VARCHAR(20),
    
    -- Datas importantes
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    due_at TIMESTAMPTZ,
    initially_assigned_at TIMESTAMPTZ,
    assigned_at TIMESTAMPTZ,
    solved_at TIMESTAMPTZ,
    
    -- Canal e origem
    via_channel VARCHAR(50),
    via_source JSONB,
    
    -- Campos customizados (JSONB para flexibilidade)
    custom_fields JSONB,
    
    -- Metadados de carga
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    _updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Índices para queries comuns
CREATE INDEX IF NOT EXISTS idx_zd_tickets_status ON core.zd_tickets (status);
CREATE INDEX IF NOT EXISTS idx_zd_tickets_priority ON core.zd_tickets (priority);
CREATE INDEX IF NOT EXISTS idx_zd_tickets_requester ON core.zd_tickets (requester_id);
CREATE INDEX IF NOT EXISTS idx_zd_tickets_assignee ON core.zd_tickets (assignee_id);
CREATE INDEX IF NOT EXISTS idx_zd_tickets_org ON core.zd_tickets (organization_id);
CREATE INDEX IF NOT EXISTS idx_zd_tickets_group ON core.zd_tickets (group_id);
CREATE INDEX IF NOT EXISTS idx_zd_tickets_created ON core.zd_tickets (created_at);
CREATE INDEX IF NOT EXISTS idx_zd_tickets_updated ON core.zd_tickets (updated_at);
CREATE INDEX IF NOT EXISTS idx_zd_tickets_solved ON core.zd_tickets (solved_at);

COMMENT ON TABLE core.zd_tickets IS 'Tickets do Zendesk normalizados';

-- -----------------------------------------------------------------------------
-- TICKETS - TAGS (tabela auxiliar many-to-many)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.zd_ticket_tags (
    ticket_id BIGINT NOT NULL,
    tag TEXT NOT NULL,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (ticket_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_zd_ticket_tags_tag ON core.zd_ticket_tags (tag);

COMMENT ON TABLE core.zd_ticket_tags IS 'Tags dos tickets do Zendesk (many-to-many)';

-- -----------------------------------------------------------------------------
-- TICKETS - COLLABORATORS (tabela auxiliar)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.zd_ticket_collaborators (
    ticket_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (ticket_id, user_id)
);

COMMENT ON TABLE core.zd_ticket_collaborators IS 'Colaboradores dos tickets (CCs)';

-- -----------------------------------------------------------------------------
-- TICKETS - FOLLOWERS (tabela auxiliar)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.zd_ticket_followers (
    ticket_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (ticket_id, user_id)
);

COMMENT ON TABLE core.zd_ticket_followers IS 'Seguidores dos tickets';

-- -----------------------------------------------------------------------------
-- TICKETS - CUSTOM FIELDS (tabela auxiliar normalizada)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.zd_ticket_custom_fields (
    ticket_id BIGINT NOT NULL,
    field_id BIGINT NOT NULL,
    value TEXT,                            -- Valor como texto
    value_json JSONB,                      -- Valor se for complexo
    
    _source_run_id VARCHAR(100),
    _loaded_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (ticket_id, field_id)
);

CREATE INDEX IF NOT EXISTS idx_zd_tcf_field ON core.zd_ticket_custom_fields (field_id);

COMMENT ON TABLE core.zd_ticket_custom_fields IS 'Campos customizados dos tickets normalizados';

-- =============================================================================
-- VIEWS ÚTEIS PARA ANÁLISE
-- =============================================================================

-- View com métricas de tempo de resolução
CREATE OR REPLACE VIEW core.vw_zd_ticket_metrics AS
SELECT 
    t.ticket_id,
    t.status,
    t.priority,
    t.created_at,
    t.solved_at,
    t.updated_at,
    
    -- Tempo até solução (em horas)
    CASE 
        WHEN t.solved_at IS NOT NULL THEN
            EXTRACT(EPOCH FROM (t.solved_at - t.created_at)) / 3600.0
        ELSE NULL
    END AS hours_to_resolution,
    
    -- Tempo desde criação
    EXTRACT(EPOCH FROM (NOW() - t.created_at)) / 3600.0 AS hours_since_created,
    
    -- Requester
    t.requester_id,
    u.name AS requester_name,
    u.email AS requester_email,
    
    -- Organization
    t.organization_id,
    o.name AS organization_name,
    
    -- Assignee
    t.assignee_id,
    a.name AS assignee_name,
    
    -- Group
    t.group_id,
    g.name AS group_name

FROM core.zd_tickets t
LEFT JOIN core.zd_users u ON t.requester_id = u.user_id
LEFT JOIN core.zd_users a ON t.assignee_id = a.user_id
LEFT JOIN core.zd_organizations o ON t.organization_id = o.organization_id
LEFT JOIN core.zd_groups g ON t.group_id = g.group_id;

COMMENT ON VIEW core.vw_zd_ticket_metrics IS 'View enriquecida de tickets com métricas de tempo';

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================
SELECT 
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'core' 
  AND table_name LIKE 'zd_%'
ORDER BY table_type, table_name;
