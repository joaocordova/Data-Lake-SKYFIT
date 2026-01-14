# -*- coding: utf-8 -*-
"""
Loader OTIMIZADO: EVO Sales Bronze → STG (PostgreSQL)

Usa COPY + Temp Table para performance máxima.
Extrai metadados do path do arquivo para rastreabilidade.

SUPORTA MÚLTIPLOS RUN_IDS (para casos de extração interrompida)

Uso:
    cd C:/skyfit-datalake/evo_sales
    
    # Processa run_id mais recente
    python src/loaders/load_evo_sales_stg_fast.py --workers 4
    
    # Processa TODOS os run_ids encontrados
    python src/loaders/load_evo_sales_stg_fast.py --workers 4 --all-runs
    
    # Processa run_id específico
    python src/loaders/load_evo_sales_stg_fast.py --workers 4 --run-id 20260109T203707Z
"""
import argparse
import gzip
import io
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Silencia logs do Azure
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


def load_env():
    env_paths = [
        PROJECT_ROOT / "config" / ".env",
        PROJECT_ROOT / ".env",
        PROJECT_ROOT.parent / "config" / ".env",
        Path(r"C:\skyfit-datalake\config\.env"),
    ]
    try:
        from dotenv import load_dotenv
        for p in env_paths:
            if p.exists():
                load_dotenv(p, override=True)
                print(f"[ENV] Carregado: {p}")
                return True
    except ImportError:
        print("[ENV] ERRO: python-dotenv não instalado")
        sys.exit(1)
    return False


load_env()

ENV = {
    "AZURE_STORAGE_ACCOUNT_NAME": os.environ.get("AZURE_STORAGE_ACCOUNT") or os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
    "AZURE_STORAGE_ACCOUNT_KEY": os.environ.get("AZURE_STORAGE_KEY") or os.environ.get("AZURE_STORAGE_ACCOUNT_KEY"),
    "AZURE_CONTAINER_NAME": os.environ.get("ADLS_CONTAINER") or os.environ.get("AZURE_CONTAINER_NAME", "datalake"),
    "PG_HOST": os.environ.get("PG_HOST"),
    "PG_PORT": os.environ.get("PG_PORT", "5432"),
    "PG_DATABASE": os.environ.get("PG_DATABASE", "postgres"),
    "PG_USER": os.environ.get("PG_USER"),
    "PG_PASSWORD": os.environ.get("PG_PASSWORD"),
    "PG_SSLMODE": os.environ.get("PG_SSLMODE", "require"),
}

# Validação
def validate_env():
    errors = []
    if not ENV.get("AZURE_STORAGE_ACCOUNT_NAME"):
        errors.append("AZURE_STORAGE_ACCOUNT não definida")
    if not ENV.get("AZURE_STORAGE_ACCOUNT_KEY"):
        errors.append("AZURE_STORAGE_KEY não definida")
    key_len = len(ENV.get("AZURE_STORAGE_ACCOUNT_KEY", ""))
    if key_len < 50:
        errors.append(f"AZURE_STORAGE_KEY parece truncada (len={key_len})")
    if not ENV.get("PG_HOST"):
        errors.append("PG_HOST não definida")
    
    if errors:
        print("[ENV] ERROS:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    
    print(f"[ENV] Azure Account: {ENV['AZURE_STORAGE_ACCOUNT_NAME']}")
    print(f"[ENV] Azure Container: {ENV['AZURE_CONTAINER_NAME']}")
    print(f"[ENV] Azure Key length: {key_len}")

validate_env()

log_file = LOG_DIR / f"load_sales_fast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class SimpleLakeClient:
    def __init__(self):
        from azure.storage.filedatalake import DataLakeServiceClient
        self.service = DataLakeServiceClient(
            account_url=f"https://{ENV['AZURE_STORAGE_ACCOUNT_NAME']}.dfs.core.windows.net",
            credential=ENV["AZURE_STORAGE_ACCOUNT_KEY"]
        )
        self.fs = self.service.get_file_system_client(ENV["AZURE_CONTAINER_NAME"])
    
    def list_paths(self, path: str) -> List[str]:
        paths = []
        for item in self.fs.get_paths(path=path, recursive=True):
            if not item.is_directory and item.name.endswith('.jsonl.gz'):
                paths.append(item.name)
        return sorted(paths)
    
    def read_gzip_jsonl(self, path: str) -> List[Dict]:
        file_client = self.fs.get_file_client(path)
        compressed = file_client.download_file().readall()
        decompressed = gzip.decompress(compressed)
        lines = decompressed.decode('utf-8').strip().split('\n')
        return [json.loads(line) for line in lines if line.strip()]


class FastSalesLoader:
    def __init__(self, workers: int = 2):
        self.lake = SimpleLakeClient()
        self.workers = workers
        self.base_path = "bronze/evo/entity=sales"
    
    def get_pg_connection(self):
        return psycopg2.connect(
            host=ENV["PG_HOST"], port=ENV["PG_PORT"], database=ENV["PG_DATABASE"],
            user=ENV["PG_USER"], password=ENV["PG_PASSWORD"], sslmode=ENV["PG_SSLMODE"]
        )
    
    def find_all_run_ids(self) -> List[str]:
        """Encontra TODOS os run_ids disponíveis."""
        paths = self.lake.list_paths(self.base_path)
        run_ids = set()
        for p in paths:
            match = re.search(r"run_id=([^/]+)", p)
            if match:
                run_ids.add(match.group(1))
        return sorted(run_ids)
    
    def find_latest_run_id(self) -> Optional[str]:
        """Encontra o run_id mais recente."""
        run_ids = self.find_all_run_ids()
        return run_ids[-1] if run_ids else None
    
    def list_parts(self, run_id: str) -> List[str]:
        paths = self.lake.list_paths(self.base_path)
        return [p for p in paths if f"run_id={run_id}" in p]
    
    def list_all_parts(self) -> List[str]:
        """Lista TODOS os parts de TODOS os run_ids."""
        return self.lake.list_paths(self.base_path)
    
    def extract_metadata(self, path: str) -> Tuple[str, str]:
        """Extrai run_id e ingestion_date do path."""
        run_match = re.search(r"run_id=([^/]+)", path)
        date_match = re.search(r"ingestion_date=(\d{4}-\d{2}-\d{2})", path)
        run_id = run_match.group(1) if run_match else "unknown"
        ing_date = date_match.group(1) if date_match else datetime.now().strftime("%Y-%m-%d")
        return run_id, ing_date
    
    def prepare_copy_data(self, records: List[Dict], source_file: str) -> io.StringIO:
        buffer = io.StringIO()
        now = datetime.now(timezone.utc).isoformat()
        run_id, ing_date = self.extract_metadata(source_file)
        
        for rec in records:
            sale_id = rec.get("idSale")
            if sale_id is None:
                continue
            
            raw_json = json.dumps(rec, ensure_ascii=False, default=str)
            # Escape TSV
            raw_json = raw_json.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
            
            buffer.write(f"{sale_id}\t{raw_json}\t{source_file}\t{run_id}\t{ing_date}\t{now}\n")
        
        buffer.seek(0)
        return buffer
    
    def load_batch_with_copy(self, conn, records: List[Dict], source_file: str) -> int:
        if not records:
            return 0
        
        cur = conn.cursor()
        try:
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS tmp_sales (
                    sale_id BIGINT,
                    raw_data JSONB,
                    _source_file TEXT,
                    run_id TEXT,
                    ingestion_date DATE,
                    _loaded_at TIMESTAMPTZ
                ) ON COMMIT DROP
            """)
            cur.execute("TRUNCATE tmp_sales")
            
            buffer = self.prepare_copy_data(records, source_file)
            cur.copy_from(buffer, 'tmp_sales', sep='\t', 
                         columns=('sale_id', 'raw_data', '_source_file', 'run_id', 'ingestion_date', '_loaded_at'))
            
            cur.execute("""
                INSERT INTO stg_evo.sales_raw (sale_id, raw_data, _source_file, run_id, ingestion_date, _loaded_at, _updated_at)
                SELECT sale_id, raw_data, _source_file, run_id, ingestion_date, _loaded_at, _loaded_at
                FROM tmp_sales
                ON CONFLICT (sale_id) DO UPDATE SET
                    raw_data = EXCLUDED.raw_data,
                    _source_file = EXCLUDED._source_file,
                    run_id = EXCLUDED.run_id,
                    ingestion_date = EXCLUDED.ingestion_date,
                    _updated_at = EXCLUDED._loaded_at
            """)
            
            count = cur.rowcount
            conn.commit()
            return count
        except Exception as e:
            conn.rollback()
            raise
        finally:
            cur.close()
    
    def process_part(self, part_path: str) -> Tuple[str, int, float]:
        """Processa um part com retry em caso de failover."""
        max_retries = 5
        base_delay = 30  # segundos
        
        for attempt in range(max_retries):
            try:
                start = datetime.now()
                records = self.lake.read_gzip_jsonl(part_path)
                conn = self.get_pg_connection()
                try:
                    count = self.load_batch_with_copy(conn, records, part_path)
                finally:
                    conn.close()
                elapsed = (datetime.now() - start).total_seconds()
                return part_path, count, elapsed
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # Detecta erros de failover/read-only
                is_failover = any(x in error_msg for x in [
                    'read-only',
                    'readonly', 
                    'connection already closed',
                    'adminshutdown',
                    'server closed the connection',
                    'terminating connection'
                ])
                
                if is_failover and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff: 30, 60, 120, 240s
                    logger.warning(f"⚠️ Failover detectado em {Path(part_path).name}, aguardando {delay}s... (tentativa {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    raise
    
    def run(self, run_id: Optional[str] = None, all_runs: bool = False) -> Dict[str, Any]:
        logger.info("="*60)
        logger.info("EVO SALES - LOADER OTIMIZADO (COPY)")
        logger.info("="*60)
        
        # Determina quais parts processar
        if all_runs:
            run_ids = self.find_all_run_ids()
            logger.info(f"Modo: TODOS os run_ids ({len(run_ids)} encontrados)")
            for rid in run_ids:
                logger.info(f"  - {rid}")
            parts = self.list_all_parts()
        elif run_id:
            logger.info(f"Run ID especificado: {run_id}")
            parts = self.list_parts(run_id)
        else:
            run_id = self.find_latest_run_id()
            if not run_id:
                logger.error("Nenhum run_id encontrado")
                return {"error": "No run_id found"}
            logger.info(f"Run ID mais recente: {run_id}")
            parts = self.list_parts(run_id)
        
        logger.info(f"Parts: {len(parts)}")
        
        if not parts:
            return {"error": "No parts found", "run_id": run_id}
        
        total_records = 0
        processed = 0
        errors = 0
        start_time = datetime.now()
        
        if self.workers > 1:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = {executor.submit(self.process_part, p): p for p in parts}
                for future in as_completed(futures):
                    try:
                        path, count, elapsed = future.result()
                        total_records += count
                        processed += 1
                        rate = count / elapsed if elapsed > 0 else 0
                        logger.info(f"  [{processed}/{len(parts)}] {Path(path).name}: {count:,} ({rate:,.0f}/s)")
                    except Exception as e:
                        errors += 1
                        logger.error(f"Erro: {e}")
        else:
            for part in parts:
                try:
                    path, count, elapsed = self.process_part(part)
                    total_records += count
                    processed += 1
                    logger.info(f"  [{processed}/{len(parts)}] {Path(path).name}: {count:,}")
                except Exception as e:
                    errors += 1
                    logger.error(f"Erro em {part}: {e}")
        
        total_elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info("="*60)
        logger.info(f"Total: {total_records:,} em {total_elapsed:.1f}s ({total_records/total_elapsed:,.0f}/s)")
        if errors:
            logger.warning(f"Erros: {errors}")
        
        return {
            "run_id": run_id if not all_runs else "ALL",
            "parts": processed,
            "records": total_records,
            "seconds": total_elapsed,
            "errors": errors
        }


def main():
    parser = argparse.ArgumentParser(description="Loader otimizado para EVO Sales")
    parser.add_argument("--run-id", help="Run ID específico")
    parser.add_argument("--all-runs", action="store_true", help="Processa TODOS os run_ids encontrados")
    parser.add_argument("--workers", type=int, default=4, help="Workers paralelos (default: 4)")
    args = parser.parse_args()
    
    result = FastSalesLoader(workers=args.workers).run(run_id=args.run_id, all_runs=args.all_runs)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
