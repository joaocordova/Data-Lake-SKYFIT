-- ============================================================================
-- SKYFIT DATA LAKE - ANÁLISE E OTIMIZAÇÃO DO POSTGRESQL
-- ============================================================================
-- Execute este script no seu cliente SQL (Azure Data Studio, pgAdmin, etc.)
-- ============================================================================

-- ============================================================================
-- 1. DIAGNÓSTICO: Tamanho por tabela
-- ============================================================================
SELECT 
    schemaname || '.' || tablename AS tabela,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS total,
    pg_size_pretty(pg_relation_size(schemaname || '.' || tablename)) AS dados,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename) - pg_relation_size(schemaname || '.' || tablename)) AS indices_toast,
    ROUND(100.0 * pg_relation_size(schemaname || '.' || tablename) / NULLIF(pg_total_relation_size(schemaname || '.' || tablename), 0), 1) AS pct_dados
FROM pg_tables 
WHERE schemaname IN ('stg_evo', 'core', 'analytics')
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;

-- ============================================================================
-- 2. DIAGNÓSTICO: Índices bloated (ocupando muito espaço)
-- ============================================================================
SELECT 
    schemaname || '.' || tablename AS tabela,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS tamanho_idx,
    idx_scan AS leituras,
    CASE WHEN idx_scan = 0 THEN '⚠️ NUNCA USADO' ELSE '✅ Em uso' END AS status
FROM pg_stat_user_indexes
JOIN pg_indexes ON indexrelname = indexname
WHERE schemaname IN ('stg_evo', 'core')
ORDER BY pg_relation_size(indexrelid) DESC
LIMIT 20;

-- ============================================================================
-- 3. DIAGNÓSTICO: Tabelas que precisam de VACUUM
-- ============================================================================
SELECT 
    schemaname || '.' || relname AS tabela,
    n_dead_tup AS tuplas_mortas,
    n_live_tup AS tuplas_vivas,
    ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS pct_bloat,
    last_vacuum,
    last_autovacuum,
    last_analyze
FROM pg_stat_user_tables
WHERE schemaname IN ('stg_evo', 'core')
ORDER BY n_dead_tup DESC;

-- ============================================================================
-- 4. OTIMIZAÇÃO: VACUUM ANALYZE nas tabelas principais
-- ============================================================================
-- EXECUTAR UM POR VEZ (pode demorar!)

-- STG Tables (mais bloated)
VACUUM (VERBOSE, ANALYZE) stg_evo.sales_raw;
VACUUM (VERBOSE, ANALYZE) stg_evo.members_raw;
VACUUM (VERBOSE, ANALYZE) stg_evo.prospects_raw;

-- CORE Tables
VACUUM (VERBOSE, ANALYZE) core.evo_sales;
VACUUM (VERBOSE, ANALYZE) core.evo_sale_items;
VACUUM (VERBOSE, ANALYZE) core.evo_receivables;
VACUUM (VERBOSE, ANALYZE) core.evo_members;
VACUUM (VERBOSE, ANALYZE) core.evo_member_memberships;
VACUUM (VERBOSE, ANALYZE) core.evo_member_contacts;
VACUUM (VERBOSE, ANALYZE) core.evo_prospects;

-- ============================================================================
-- 5. OTIMIZAÇÃO: REINDEX para reconstruir índices bloated
-- ============================================================================
-- ATENÇÃO: REINDEX bloqueia a tabela! Executar em janela de manutenção

-- Opção 1: REINDEX CONCURRENTLY (não bloqueia, mas mais lento)
REINDEX INDEX CONCURRENTLY stg_evo.sales_raw_pkey;
REINDEX INDEX CONCURRENTLY stg_evo.members_raw_pkey;

-- Opção 2: REINDEX completo da tabela (mais rápido, mas bloqueia)
-- REINDEX TABLE stg_evo.sales_raw;
-- REINDEX TABLE stg_evo.members_raw;

-- ============================================================================
-- 6. OTIMIZAÇÃO: Remover índices não utilizados nas tabelas STG
-- ============================================================================
-- As tabelas STG só precisam do índice PK para UPSERT
-- Índices adicionais são desnecessários e ocupam espaço

-- Lista índices candidatos a remoção (nunca usados)
SELECT 
    'DROP INDEX IF EXISTS ' || schemaname || '.' || indexname || ';' AS comando
FROM pg_stat_user_indexes
JOIN pg_indexes ON indexrelname = indexname
WHERE schemaname = 'stg_evo'
  AND idx_scan = 0
  AND indexname NOT LIKE '%_pkey';

-- ============================================================================
-- 7. OTIMIZAÇÃO: Compressão TOAST para JSONB
-- ============================================================================
-- Verifica configuração de compressão
SELECT 
    c.relname AS tabela,
    a.attname AS coluna,
    a.attstorage AS storage,
    CASE a.attstorage 
        WHEN 'p' THEN 'plain (sem compressão)'
        WHEN 'e' THEN 'external (sem compressão)'
        WHEN 'm' THEN 'main (compressão inline)'
        WHEN 'x' THEN 'extended (compressão + TOAST)' -- IDEAL para JSONB
    END AS descricao
FROM pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname IN ('stg_evo', 'core')
  AND a.attname = 'raw_data'
  AND a.atttypid = 'jsonb'::regtype;

-- Se não estiver como 'x' (extended), alterar:
-- ALTER TABLE stg_evo.sales_raw ALTER COLUMN raw_data SET STORAGE EXTENDED;

-- ============================================================================
-- 8. LIMPEZA AGRESSIVA: pg_repack (se instalado)
-- ============================================================================
-- pg_repack reorganiza tabelas sem bloquear
-- Precisa instalar: CREATE EXTENSION pg_repack;

-- Verificar se está instalado
SELECT * FROM pg_extension WHERE extname = 'pg_repack';

-- Se instalado, executar no terminal:
-- pg_repack -d seu_database -t stg_evo.sales_raw
-- pg_repack -d seu_database -t stg_evo.members_raw

-- ============================================================================
-- 9. ALTERNATIVA RADICAL: Recriar tabelas STG (recupera TODO espaço)
-- ============================================================================
-- ATENÇÃO: Só fazer se puder recarregar os dados!

-- Opção: CREATE TABLE AS SELECT (cria nova tabela compacta)
/*
CREATE TABLE stg_evo.sales_raw_new AS 
SELECT * FROM stg_evo.sales_raw;

-- Adiciona constraint
ALTER TABLE stg_evo.sales_raw_new ADD PRIMARY KEY (sale_id);

-- Troca as tabelas
BEGIN;
ALTER TABLE stg_evo.sales_raw RENAME TO sales_raw_old;
ALTER TABLE stg_evo.sales_raw_new RENAME TO sales_raw;
COMMIT;

-- Remove antiga (libera espaço)
DROP TABLE stg_evo.sales_raw_old;
*/

-- ============================================================================
-- 10. CONFIGURAÇÃO: Ajustes de storage do Azure PostgreSQL
-- ============================================================================
-- No Azure Portal, considerar:
-- 1. Habilitar "Storage Auto-grow" se não estiver
-- 2. Usar tier com IOPS maiores se I/O for gargalo
-- 3. Configurar maintenance_work_mem maior para VACUUM

-- Verificar configurações atuais
SHOW maintenance_work_mem;
SHOW autovacuum_vacuum_scale_factor;
SHOW autovacuum_analyze_scale_factor;

-- Recomendações para tabelas grandes:
-- ALTER SYSTEM SET maintenance_work_mem = '1GB';
-- ALTER SYSTEM SET autovacuum_vacuum_scale_factor = 0.1;
-- SELECT pg_reload_conf();

-- ============================================================================
-- 11. MONITORAMENTO: Query para acompanhar progresso
-- ============================================================================
SELECT 
    schemaname || '.' || relname AS tabela,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS tamanho,
    n_live_tup AS registros,
    ROUND(pg_total_relation_size(schemaname || '.' || relname)::numeric / NULLIF(n_live_tup, 0), 0) AS bytes_por_registro
FROM pg_stat_user_tables
WHERE schemaname IN ('stg_evo', 'core')
ORDER BY pg_total_relation_size(schemaname || '.' || relname) DESC;

-- ============================================================================
-- 12. ESTIMATIVA DE ECONOMIA
-- ============================================================================
-- Baseado nos seus dados:
-- stg_evo.sales_raw: 123 GB (103 GB índices!) 
-- Após REINDEX + VACUUM: ~30-40 GB (economia de ~80 GB)
--
-- stg_evo.members_raw: 15 GB (13 GB índices!)
-- Após REINDEX + VACUUM: ~4-5 GB (economia de ~10 GB)
--
-- TOTAL ESTIMADO: Economia de ~90 GB
-- ============================================================================
