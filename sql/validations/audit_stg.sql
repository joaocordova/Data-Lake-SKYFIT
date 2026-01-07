-- =============================================================================
-- SKYFIT DATA LAKE - QUERIES DE AUDITORIA STG
-- =============================================================================
-- Queries para validar a carga no staging (Bronze -> STG)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. CONTAGEM GERAL POR TABELA E RUN
-- -----------------------------------------------------------------------------

-- Zendesk: contagem por tabela e run
SELECT 
    'tickets' AS entity,
    run_id,
    ingestion_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT source_blob_path) AS distinct_files,
    MIN(loaded_at) AS first_load,
    MAX(loaded_at) AS last_load
FROM stg_zendesk.tickets_raw
GROUP BY run_id, ingestion_date

UNION ALL

SELECT 
    'users' AS entity,
    run_id,
    ingestion_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT source_blob_path) AS distinct_files,
    MIN(loaded_at) AS first_load,
    MAX(loaded_at) AS last_load
FROM stg_zendesk.users_raw
GROUP BY run_id, ingestion_date

UNION ALL

SELECT 
    'organizations' AS entity,
    run_id,
    ingestion_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT source_blob_path) AS distinct_files,
    MIN(loaded_at) AS first_load,
    MAX(loaded_at) AS last_load
FROM stg_zendesk.organizations_raw
GROUP BY run_id, ingestion_date

ORDER BY entity, ingestion_date DESC, run_id DESC;


-- Pipedrive: contagem por scope, tabela e run
SELECT 
    'deals' AS entity,
    scope,
    run_id,
    ingestion_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT source_blob_path) AS distinct_files
FROM stg_pipedrive.deals_raw
GROUP BY scope, run_id, ingestion_date

UNION ALL

SELECT 
    'persons' AS entity,
    scope,
    run_id,
    ingestion_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT source_blob_path) AS distinct_files
FROM stg_pipedrive.persons_raw
GROUP BY scope, run_id, ingestion_date

UNION ALL

SELECT 
    'organizations' AS entity,
    scope,
    run_id,
    ingestion_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT source_blob_path) AS distinct_files
FROM stg_pipedrive.organizations_raw
GROUP BY scope, run_id, ingestion_date

ORDER BY scope, entity, ingestion_date DESC;


-- -----------------------------------------------------------------------------
-- 2. VERIFICAÇÃO DE DUPLICATAS
-- -----------------------------------------------------------------------------

-- Zendesk: verifica se há duplicatas por (source_blob_path, source_line_no)
-- Deveria retornar 0 se a constraint UNIQUE está funcionando
SELECT 
    'stg_zendesk.tickets_raw' AS table_name,
    source_blob_path,
    source_line_no,
    COUNT(*) AS occurrences
FROM stg_zendesk.tickets_raw
GROUP BY source_blob_path, source_line_no
HAVING COUNT(*) > 1;

-- Pipedrive: verifica duplicatas por (scope, source_blob_path, source_line_no)
SELECT 
    'stg_pipedrive.deals_raw' AS table_name,
    scope,
    source_blob_path,
    source_line_no,
    COUNT(*) AS occurrences
FROM stg_pipedrive.deals_raw
GROUP BY scope, source_blob_path, source_line_no
HAVING COUNT(*) > 1;


-- -----------------------------------------------------------------------------
-- 3. VERIFICAÇÃO DE IDs ÚNICOS NO PAYLOAD
-- -----------------------------------------------------------------------------

-- Zendesk: conta IDs únicos vs total de registros
SELECT 
    'tickets' AS entity,
    COUNT(*) AS total_records,
    COUNT(DISTINCT payload->>'id') AS unique_ids,
    COUNT(*) - COUNT(DISTINCT payload->>'id') AS potential_duplicates
FROM stg_zendesk.tickets_raw

UNION ALL

SELECT 
    'users' AS entity,
    COUNT(*) AS total_records,
    COUNT(DISTINCT payload->>'id') AS unique_ids,
    COUNT(*) - COUNT(DISTINCT payload->>'id') AS potential_duplicates
FROM stg_zendesk.users_raw

UNION ALL

SELECT 
    'organizations' AS entity,
    COUNT(*) AS total_records,
    COUNT(DISTINCT payload->>'id') AS unique_ids,
    COUNT(*) - COUNT(DISTINCT payload->>'id') AS potential_duplicates
FROM stg_zendesk.organizations_raw;


-- -----------------------------------------------------------------------------
-- 4. VALIDAÇÃO DE CAMPOS OBRIGATÓRIOS NO PAYLOAD
-- -----------------------------------------------------------------------------

-- Zendesk tickets: verifica campos nulos que não deveriam ser
SELECT 
    'tickets_sem_id' AS check_name,
    COUNT(*) AS count
FROM stg_zendesk.tickets_raw
WHERE payload->>'id' IS NULL OR payload->>'id' = ''

UNION ALL

SELECT 
    'tickets_sem_status' AS check_name,
    COUNT(*) AS count
FROM stg_zendesk.tickets_raw
WHERE payload->>'status' IS NULL OR payload->>'status' = ''

UNION ALL

SELECT 
    'tickets_sem_created_at' AS check_name,
    COUNT(*) AS count
FROM stg_zendesk.tickets_raw
WHERE payload->>'created_at' IS NULL;


-- -----------------------------------------------------------------------------
-- 5. HISTÓRICO DE CARGAS (últimos 7 dias)
-- -----------------------------------------------------------------------------

SELECT 
    DATE(loaded_at) AS load_date,
    'zendesk_tickets' AS source,
    COUNT(*) AS records_loaded
FROM stg_zendesk.tickets_raw
WHERE loaded_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(loaded_at)

UNION ALL

SELECT 
    DATE(loaded_at) AS load_date,
    'pipedrive_deals' AS source,
    COUNT(*) AS records_loaded
FROM stg_pipedrive.deals_raw
WHERE loaded_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(loaded_at)

ORDER BY load_date DESC, source;


-- -----------------------------------------------------------------------------
-- 6. VERIFICAÇÃO DE DATAS INVÁLIDAS
-- -----------------------------------------------------------------------------

-- Zendesk: tickets com created_at > updated_at (anomalia)
SELECT 
    payload->>'id' AS ticket_id,
    payload->>'created_at' AS created_at,
    payload->>'updated_at' AS updated_at
FROM stg_zendesk.tickets_raw
WHERE (payload->>'created_at')::timestamptz > (payload->>'updated_at')::timestamptz;


-- -----------------------------------------------------------------------------
-- 7. SAMPLE DE DADOS (para inspeção visual)
-- -----------------------------------------------------------------------------

-- Amostra de tickets mais recentes
SELECT 
    payload->>'id' AS ticket_id,
    payload->>'subject' AS subject,
    payload->>'status' AS status,
    payload->>'created_at' AS created_at,
    run_id,
    loaded_at
FROM stg_zendesk.tickets_raw
ORDER BY loaded_at DESC
LIMIT 10;

-- Amostra de deals mais recentes
SELECT 
    scope,
    payload->>'id' AS deal_id,
    payload->>'title' AS title,
    payload->>'status' AS status,
    payload->>'value' AS value,
    run_id,
    loaded_at
FROM stg_pipedrive.deals_raw
ORDER BY loaded_at DESC
LIMIT 10;
