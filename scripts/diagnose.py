# ============================================================================
# DIAGNÓSTICO - Execute para ver erros detalhados
# ============================================================================
# Uso: python scripts/diagnose.py
# ============================================================================

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

print("=" * 60)
print("SKYFIT DATALAKE - DIAGNÓSTICO")
print("=" * 60)

# Test 1: Imports
print("\n[1] Testando imports...")
try:
    from config.settings import azure_storage, postgres
    print("    ✓ config.settings OK")
except Exception as e:
    print(f"    ✗ config.settings ERRO: {e}")

try:
    from src.common.lake import LakeClient
    print("    ✓ src.common.lake OK")
except Exception as e:
    print(f"    ✗ src.common.lake ERRO: {e}")

try:
    from src.common.logging_config import RunLogger
    print("    ✓ src.common.logging_config OK")
except Exception as e:
    print(f"    ✗ src.common.logging_config ERRO: {e}")

# Test 2: PostgreSQL
print("\n[2] Testando conexão PostgreSQL...")
try:
    import psycopg2
    conn = psycopg2.connect(
        host=postgres.HOST,
        port=postgres.PORT,
        dbname=postgres.DATABASE,
        user=postgres.USER,
        password=postgres.PASSWORD,
        sslmode=postgres.SSLMODE
    )
    cur = conn.cursor()
    cur.execute("SELECT 1")
    print("    ✓ PostgreSQL conectado!")
    
    # Verificar schemas
    cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('stg_pipedrive', 'stg_zendesk', 'core')")
    schemas = [r[0] for r in cur.fetchall()]
    print(f"    ✓ Schemas encontrados: {schemas}")
    
    conn.close()
except Exception as e:
    print(f"    ✗ PostgreSQL ERRO: {e}")

# Test 3: ADLS
print("\n[3] Testando conexão ADLS...")
try:
    lake = LakeClient(
        account=azure_storage.ACCOUNT,
        key=azure_storage.KEY,
        container=azure_storage.CONTAINER
    )
    # Listar manifests
    manifests = list(lake.list_blobs("_meta/pipedrive/runs/"))[:3]
    print(f"    ✓ ADLS conectado! Manifests: {len(manifests)}")
except Exception as e:
    print(f"    ✗ ADLS ERRO: {e}")

# Test 4: Executar loaders manualmente
print("\n[4] Testando load_pipedrive_stg...")
try:
    from src.loaders.load_pipedrive_stg import PipedriveStgLoader
    print("    ✓ Import PipedriveStgLoader OK")
except Exception as e:
    print(f"    ✗ Import ERRO: {e}")
    import traceback
    traceback.print_exc()

print("\n[5] Testando load_zendesk_stg...")
try:
    from src.loaders.load_zendesk_stg import ZendeskStgLoader
    print("    ✓ Import ZendeskStgLoader OK")
except Exception as e:
    print(f"    ✗ Import ERRO: {e}")
    import traceback
    traceback.print_exc()

print("\n[6] Testando normalize_pipedrive...")
try:
    from src.transformers.normalize_pipedrive import PipedriveCoreNormalizer
    print("    ✓ Import PipedriveCoreNormalizer OK")
except Exception as e:
    print(f"    ✗ Import ERRO: {e}")
    import traceback
    traceback.print_exc()

print("\n[7] Testando normalize_zendesk...")
try:
    from src.transformers.normalize_zendesk import ZendeskCoreNormalizer
    print("    ✓ Import ZendeskCoreNormalizer OK")
except Exception as e:
    print(f"    ✗ Import ERRO: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("DIAGNÓSTICO COMPLETO")
print("=" * 60)
