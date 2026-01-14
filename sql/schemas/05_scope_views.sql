-- ============================================================================
-- SKYFIT DATA LAKE - VIEWS POR SCOPE (OPCIONAL)
-- ============================================================================
-- Execute após criar as tabelas CORE para ter views isoladas por scope
-- Isso facilita queries quando você quer apenas um scope específico
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

-- Users por scope
CREATE OR REPLACE VIEW core.vw_pd_users_comercial AS
SELECT * FROM core.pd_users WHERE scope = 'comercial';

CREATE OR REPLACE VIEW core.vw_pd_users_expansao AS
SELECT * FROM core.pd_users WHERE scope = 'expansao';

-- ============================================================================
-- EXEMPLO DE USO
-- ============================================================================
-- Antes (com filtro):
--   SELECT * FROM core.pd_deals WHERE scope = 'comercial' AND status = 'won';
--
-- Depois (com view):
--   SELECT * FROM core.vw_pd_deals_comercial WHERE status = 'won';
-- ============================================================================

-- Verificar views criadas
SELECT table_name, table_type 
FROM information_schema.tables 
WHERE table_schema = 'core' AND table_type = 'VIEW'
ORDER BY table_name;
