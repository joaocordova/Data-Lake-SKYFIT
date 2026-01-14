-- ============================================================================
-- SKYFIT DATA LAKE - SCHEMA CORE (GOLD LAYER)
-- ============================================================================
-- Recria todas as tabelas CORE para Pipedrive e Zendesk
-- Execute com: psql -f sql/schemas/04_core_tables.sql
-- ============================================================================

-- Criar schema se não existir
CREATE SCHEMA IF NOT EXISTS core;

-- ============================================================================
-- PIPEDRIVE CORE TABLES
-- ============================================================================

-- Drop tables (ordem reversa por FK)
DROP TABLE IF EXISTS core.pd_activities CASCADE;
DROP TABLE IF EXISTS core.pd_deals CASCADE;
DROP TABLE IF EXISTS core.pd_persons CASCADE;
DROP TABLE IF EXISTS core.pd_organizations CASCADE;
DROP TABLE IF EXISTS core.pd_users CASCADE;
DROP TABLE IF EXISTS core.pd_stages CASCADE;
DROP TABLE IF EXISTS core.pd_pipelines CASCADE;

-- --------------------------------------------------------------------------
-- pd_pipelines (dimensão)
-- --------------------------------------------------------------------------
CREATE TABLE core.pd_pipelines (
    pipeline_id         BIGINT NOT NULL,
    scope               VARCHAR(50) NOT NULL,
    name                VARCHAR(255),
    url_title           VARCHAR(255),
    order_nr            INTEGER,
    active              BOOLEAN,
    deal_probability    BOOLEAN,
    add_time            TIMESTAMPTZ,
    update_time         TIMESTAMPTZ,
    _source_run_id      VARCHAR(50),
    _loaded_at          TIMESTAMPTZ DEFAULT NOW(),
    _updated_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, scope)
);

-- --------------------------------------------------------------------------
-- pd_stages (dimensão)
-- --------------------------------------------------------------------------
CREATE TABLE core.pd_stages (
    stage_id            BIGINT NOT NULL,
    scope               VARCHAR(50) NOT NULL,
    pipeline_id         BIGINT,
    name                VARCHAR(255),
    order_nr            INTEGER,
    active_flag         BOOLEAN,
    deal_probability    INTEGER,
    rotten_flag         BOOLEAN,
    rotten_days         INTEGER,
    add_time            TIMESTAMPTZ,
    update_time         TIMESTAMPTZ,
    _source_run_id      VARCHAR(50),
    _loaded_at          TIMESTAMPTZ DEFAULT NOW(),
    _updated_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stage_id, scope)
);

-- --------------------------------------------------------------------------
-- pd_users (dimensão)
-- --------------------------------------------------------------------------
CREATE TABLE core.pd_users (
    user_id             BIGINT NOT NULL,
    scope               VARCHAR(50) NOT NULL,
    name                VARCHAR(255),
    email               VARCHAR(255),
    active_flag         BOOLEAN,
    is_admin            BOOLEAN,
    role_id             BIGINT,
    icon_url            TEXT,
    timezone_name       VARCHAR(100),
    timezone_offset     VARCHAR(20),
    phone               VARCHAR(50),
    created             TIMESTAMPTZ,
    modified            TIMESTAMPTZ,
    _source_run_id      VARCHAR(50),
    _loaded_at          TIMESTAMPTZ DEFAULT NOW(),
    _updated_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, scope)
);

-- --------------------------------------------------------------------------
-- pd_organizations (dimensão)
-- --------------------------------------------------------------------------
CREATE TABLE core.pd_organizations (
    org_id              BIGINT NOT NULL,
    scope               VARCHAR(50) NOT NULL,
    name                VARCHAR(500),
    owner_id            BIGINT,
    address             TEXT,
    address_locality    VARCHAR(255),
    address_country     VARCHAR(100),
    address_postal_code VARCHAR(50),
    cc_email            VARCHAR(255),
    active_flag         BOOLEAN,
    people_count        INTEGER,
    open_deals_count    INTEGER,
    won_deals_count     INTEGER,
    lost_deals_count    INTEGER,
    add_time            TIMESTAMPTZ,
    update_time         TIMESTAMPTZ,
    custom_fields       JSONB,
    _source_run_id      VARCHAR(50),
    _loaded_at          TIMESTAMPTZ DEFAULT NOW(),
    _updated_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (org_id, scope)
);

-- --------------------------------------------------------------------------
-- pd_persons (dimensão)
-- --------------------------------------------------------------------------
CREATE TABLE core.pd_persons (
    person_id                   BIGINT NOT NULL,
    scope                       VARCHAR(50) NOT NULL,
    name                        VARCHAR(500),
    first_name                  VARCHAR(255),
    last_name                   VARCHAR(255),
    org_id                      BIGINT,
    owner_id                    BIGINT,
    primary_email               VARCHAR(255),
    emails                      JSONB,
    primary_phone               VARCHAR(100),
    phones                      JSONB,
    visible_to                  INTEGER,
    active_flag                 BOOLEAN,
    open_deals_count            INTEGER,
    related_open_deals_count    INTEGER,
    closed_deals_count          INTEGER,
    related_closed_deals_count  INTEGER,
    won_deals_count             INTEGER,
    related_won_deals_count     INTEGER,
    lost_deals_count            INTEGER,
    related_lost_deals_count    INTEGER,
    add_time                    TIMESTAMPTZ,
    update_time                 TIMESTAMPTZ,
    custom_fields               JSONB,
    _source_run_id              VARCHAR(50),
    _loaded_at                  TIMESTAMPTZ DEFAULT NOW(),
    _updated_at                 TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (person_id, scope)
);

CREATE INDEX idx_pd_persons_org ON core.pd_persons(org_id, scope);
CREATE INDEX idx_pd_persons_email ON core.pd_persons(primary_email);

-- --------------------------------------------------------------------------
-- pd_deals (fato)
-- --------------------------------------------------------------------------
CREATE TABLE core.pd_deals (
    deal_id                 BIGINT NOT NULL,
    scope                   VARCHAR(50) NOT NULL,
    title                   VARCHAR(500),
    value                   NUMERIC(15,2),
    currency                VARCHAR(10),
    status                  VARCHAR(20),
    person_id               BIGINT,
    org_id                  BIGINT,
    user_id                 BIGINT,
    pipeline_id             BIGINT,
    stage_id                BIGINT,
    expected_close_date     DATE,
    probability             INTEGER,
    won_time                TIMESTAMPTZ,
    lost_time               TIMESTAMPTZ,
    close_time              TIMESTAMPTZ,
    add_time                TIMESTAMPTZ,
    update_time             TIMESTAMPTZ,
    stage_change_time       TIMESTAMPTZ,
    lost_reason             TEXT,
    visible_to              INTEGER,
    activities_count        INTEGER,
    done_activities_count   INTEGER,
    undone_activities_count INTEGER,
    files_count             INTEGER,
    notes_count             INTEGER,
    followers_count         INTEGER,
    email_messages_count    INTEGER,
    products_count          INTEGER,
    next_activity_date      DATE,
    last_activity_date      DATE,
    origin                  VARCHAR(100),
    channel                 INTEGER,
    custom_fields           JSONB,
    _source_run_id          VARCHAR(50),
    _loaded_at              TIMESTAMPTZ DEFAULT NOW(),
    _updated_at             TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (deal_id, scope)
);

CREATE INDEX idx_pd_deals_status ON core.pd_deals(status, scope);
CREATE INDEX idx_pd_deals_pipeline ON core.pd_deals(pipeline_id, stage_id, scope);
CREATE INDEX idx_pd_deals_person ON core.pd_deals(person_id, scope);
CREATE INDEX idx_pd_deals_org ON core.pd_deals(org_id, scope);
CREATE INDEX idx_pd_deals_user ON core.pd_deals(user_id, scope);
CREATE INDEX idx_pd_deals_add_time ON core.pd_deals(add_time);

-- --------------------------------------------------------------------------
-- pd_activities (fato)
-- --------------------------------------------------------------------------
CREATE TABLE core.pd_activities (
    activity_id                 BIGINT NOT NULL,
    scope                       VARCHAR(50) NOT NULL,
    type                        VARCHAR(100),
    subject                     VARCHAR(500),
    note                        TEXT,
    done                        BOOLEAN,
    busy_flag                   BOOLEAN,
    user_id                     BIGINT,
    deal_id                     BIGINT,
    person_id                   BIGINT,
    org_id                      BIGINT,
    lead_id                     VARCHAR(100),
    project_id                  BIGINT,
    due_date                    DATE,
    due_time                    VARCHAR(20),
    duration                    VARCHAR(20),
    add_time                    TIMESTAMPTZ,
    marked_as_done_time         TIMESTAMPTZ,
    update_time                 TIMESTAMPTZ,
    location                    TEXT,
    location_formatted_address  TEXT,
    participants                JSONB,
    attendees                   JSONB,
    conference_meeting_client   VARCHAR(100),
    conference_meeting_url      TEXT,
    conference_meeting_id       VARCHAR(255),
    public_description          TEXT,
    active_flag                 BOOLEAN,
    _source_run_id              VARCHAR(50),
    _loaded_at                  TIMESTAMPTZ DEFAULT NOW(),
    _updated_at                 TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (activity_id, scope)
);

CREATE INDEX idx_pd_activities_deal ON core.pd_activities(deal_id, scope);
CREATE INDEX idx_pd_activities_person ON core.pd_activities(person_id, scope);
CREATE INDEX idx_pd_activities_user ON core.pd_activities(user_id, scope);
CREATE INDEX idx_pd_activities_due ON core.pd_activities(due_date);

-- ============================================================================
-- ZENDESK CORE TABLES
-- ============================================================================

DROP TABLE IF EXISTS core.zd_ticket_custom_fields CASCADE;
DROP TABLE IF EXISTS core.zd_ticket_tags CASCADE;
DROP TABLE IF EXISTS core.zd_tickets CASCADE;
DROP TABLE IF EXISTS core.zd_ticket_forms CASCADE;
DROP TABLE IF EXISTS core.zd_ticket_fields CASCADE;
DROP TABLE IF EXISTS core.zd_groups CASCADE;
DROP TABLE IF EXISTS core.zd_users CASCADE;
DROP TABLE IF EXISTS core.zd_organizations CASCADE;

-- --------------------------------------------------------------------------
-- zd_organizations (dimensão)
-- --------------------------------------------------------------------------
CREATE TABLE core.zd_organizations (
    organization_id     BIGINT NOT NULL PRIMARY KEY,
    name                VARCHAR(500),
    domain_names        JSONB,
    group_id            BIGINT,
    shared_tickets      BOOLEAN,
    shared_comments     BOOLEAN,
    external_id         VARCHAR(255),
    tags                JSONB,
    organization_fields JSONB,
    created_at          TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ,
    _source_run_id      VARCHAR(50),
    _loaded_at          TIMESTAMPTZ DEFAULT NOW(),
    _updated_at         TIMESTAMPTZ DEFAULT NOW()
);

-- --------------------------------------------------------------------------
-- zd_users (dimensão)
-- --------------------------------------------------------------------------
CREATE TABLE core.zd_users (
    user_id                 BIGINT NOT NULL PRIMARY KEY,
    name                    VARCHAR(500),
    email                   VARCHAR(255),
    phone                   VARCHAR(100),
    role                    VARCHAR(50),
    organization_id         BIGINT,
    time_zone               VARCHAR(100),
    locale                  VARCHAR(20),
    active                  BOOLEAN,
    verified                BOOLEAN,
    suspended               BOOLEAN,
    tags                    JSONB,
    user_fields             JSONB,
    external_id             VARCHAR(255),
    alias                   VARCHAR(255),
    notes                   TEXT,
    details                 TEXT,
    default_group_id        BIGINT,
    only_private_comments   BOOLEAN,
    restricted_agent        BOOLEAN,
    shared                  BOOLEAN,
    shared_agent            BOOLEAN,
    signature               TEXT,
    ticket_restriction      VARCHAR(50),
    created_at              TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ,
    last_login_at           TIMESTAMPTZ,
    _source_run_id          VARCHAR(50),
    _loaded_at              TIMESTAMPTZ DEFAULT NOW(),
    _updated_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_zd_users_email ON core.zd_users(email);
CREATE INDEX idx_zd_users_org ON core.zd_users(organization_id);

-- --------------------------------------------------------------------------
-- zd_groups (dimensão)
-- --------------------------------------------------------------------------
CREATE TABLE core.zd_groups (
    group_id            BIGINT NOT NULL PRIMARY KEY,
    name                VARCHAR(255),
    description         TEXT,
    is_public           BOOLEAN,
    deleted             BOOLEAN,
    created_at          TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ,
    _source_run_id      VARCHAR(50),
    _loaded_at          TIMESTAMPTZ DEFAULT NOW(),
    _updated_at         TIMESTAMPTZ DEFAULT NOW()
);

-- --------------------------------------------------------------------------
-- zd_ticket_fields (dimensão)
-- --------------------------------------------------------------------------
CREATE TABLE core.zd_ticket_fields (
    field_id                BIGINT NOT NULL PRIMARY KEY,
    type                    VARCHAR(100),
    title                   VARCHAR(255),
    description             TEXT,
    position                INTEGER,
    active                  BOOLEAN,
    required                BOOLEAN,
    collapsed_for_agents    BOOLEAN,
    regexp_for_validation   TEXT,
    title_in_portal         VARCHAR(255),
    visible_in_portal       BOOLEAN,
    editable_in_portal      BOOLEAN,
    required_in_portal      BOOLEAN,
    custom_field_options    JSONB,
    system_field_options    JSONB,
    created_at              TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ,
    _source_run_id          VARCHAR(50),
    _loaded_at              TIMESTAMPTZ DEFAULT NOW(),
    _updated_at             TIMESTAMPTZ DEFAULT NOW()
);

-- --------------------------------------------------------------------------
-- zd_ticket_forms (dimensão)
-- --------------------------------------------------------------------------
CREATE TABLE core.zd_ticket_forms (
    form_id                 BIGINT NOT NULL PRIMARY KEY,
    name                    VARCHAR(255),
    display_name            VARCHAR(255),
    position                INTEGER,
    active                  BOOLEAN,
    default_form            BOOLEAN,
    end_user_visible        BOOLEAN,
    in_all_brands           BOOLEAN,
    restricted_brand_ids    JSONB,
    ticket_field_ids        JSONB,
    created_at              TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ,
    _source_run_id          VARCHAR(50),
    _loaded_at              TIMESTAMPTZ DEFAULT NOW(),
    _updated_at             TIMESTAMPTZ DEFAULT NOW()
);

-- --------------------------------------------------------------------------
-- zd_tickets (fato)
-- --------------------------------------------------------------------------
CREATE TABLE core.zd_tickets (
    ticket_id           BIGINT NOT NULL PRIMARY KEY,
    subject             VARCHAR(500),
    description         TEXT,
    status              VARCHAR(50),
    priority            VARCHAR(20),
    type                VARCHAR(50),
    requester_id        BIGINT,
    submitter_id        BIGINT,
    assignee_id         BIGINT,
    organization_id     BIGINT,
    group_id            BIGINT,
    brand_id            BIGINT,
    ticket_form_id      BIGINT,
    external_id         VARCHAR(255),
    via_channel         VARCHAR(100),
    via_source          JSONB,
    is_public           BOOLEAN,
    has_incidents       BOOLEAN,
    allow_channelback   BOOLEAN,
    allow_attachments   BOOLEAN,
    tags                JSONB,
    custom_fields       JSONB,
    created_at          TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ,
    _source_run_id      VARCHAR(50),
    _loaded_at          TIMESTAMPTZ DEFAULT NOW(),
    _updated_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_zd_tickets_status ON core.zd_tickets(status);
CREATE INDEX idx_zd_tickets_requester ON core.zd_tickets(requester_id);
CREATE INDEX idx_zd_tickets_assignee ON core.zd_tickets(assignee_id);
CREATE INDEX idx_zd_tickets_org ON core.zd_tickets(organization_id);
CREATE INDEX idx_zd_tickets_created ON core.zd_tickets(created_at);

-- --------------------------------------------------------------------------
-- zd_ticket_tags (tabela derivada)
-- --------------------------------------------------------------------------
CREATE TABLE core.zd_ticket_tags (
    ticket_id           BIGINT NOT NULL,
    tag                 VARCHAR(255) NOT NULL,
    _loaded_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ticket_id, tag)
);

-- --------------------------------------------------------------------------
-- zd_ticket_custom_fields (tabela derivada)
-- --------------------------------------------------------------------------
CREATE TABLE core.zd_ticket_custom_fields (
    ticket_id           BIGINT NOT NULL,
    field_id            BIGINT NOT NULL,
    value               TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ticket_id, field_id)
);

-- ============================================================================
-- GRANTS
-- ============================================================================
GRANT USAGE ON SCHEMA core TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO PUBLIC;

-- ============================================================================
-- VIEWS POR SCOPE (Pipedrive) - Para facilitar consultas isoladas
-- ============================================================================

-- Deals por scope
CREATE OR REPLACE VIEW core.vw_pd_deals_comercial AS
SELECT * FROM core.pd_deals WHERE scope = 'comercial';

CREATE OR REPLACE VIEW core.vw_pd_deals_expansao AS
SELECT * FROM core.pd_deals WHERE scope = 'expansao';

-- Persons por scope
CREATE OR REPLACE VIEW core.vw_pd_persons_comercial AS
SELECT * FROM core.pd_persons WHERE scope = 'comercial';

CREATE OR REPLACE VIEW core.vw_pd_persons_expansao AS
SELECT * FROM core.pd_persons WHERE scope = 'expansao';

-- Activities por scope
CREATE OR REPLACE VIEW core.vw_pd_activities_comercial AS
SELECT * FROM core.pd_activities WHERE scope = 'comercial';

CREATE OR REPLACE VIEW core.vw_pd_activities_expansao AS
SELECT * FROM core.pd_activities WHERE scope = 'expansao';

-- Organizations por scope
CREATE OR REPLACE VIEW core.vw_pd_organizations_comercial AS
SELECT * FROM core.pd_organizations WHERE scope = 'comercial';

CREATE OR REPLACE VIEW core.vw_pd_organizations_expansao AS
SELECT * FROM core.pd_organizations WHERE scope = 'expansao';

-- ============================================================================
-- VERIFICAÇÃO
-- ============================================================================
DO $$
DECLARE
    tbl_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO tbl_count
    FROM information_schema.tables 
    WHERE table_schema = 'core';
    
    RAISE NOTICE 'Schema CORE criado com % tabelas', tbl_count;
END $$;
