# -*- coding: utf-8 -*-
"""
Loader OTIMIZADO: EVO Entries Bronze → STG

Usa COPY + Temp Table para performance máxima.
Otimizado para ALTO VOLUME (~110M registros).

IMPORTANTE: Tabela particionada por ano para melhor performance

Uso:
    cd C:/skyfit-datalake/evo_entries
    python src/loaders/load_evo_entries_stg_fast.py --workers 4 --all-runs
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

# Silencia logs
logging.getLogger("azure").setLevel(logging.WARNING)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)


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


def validate_env():
    errors = []
    if not ENV.get("AZURE_STORAGE_ACCOUNT_NAME"):
        errors.append("AZURE_STORAGE_ACCOUNT não definida")
    key_len = len(ENV.get("AZURE_STORAGE_ACCOUNT_KEY", ""))
    if key_len < 50:
        errors.append(f"AZURE_STORAGE_KEY truncada (len={key_len})")
    if not ENV.get("PG_HOST"):
        errors.append("PG_HOST não definida")
    
    if errors:
        print("[ENV] ERROS:", errors)
        sys.exit(1)
    
    print(f"[ENV] Azure: {ENV['AZURE_STORAGE_ACCOUNT_NAME']}")
    print(f"[ENV] Key length: {key_len}")

validate_env()

log_file = LOG_DIR / f"load_entries_fast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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


class FastEntriesLoader:
    def __init__(self, workers: int = 4):
        self.lake = SimpleLakeClient()
        self.workers = workers
        self.base_path = "bronze/evo/entity=entries"
    
    def get_pg_connection(self):
        return psycopg2.connect(
            host=ENV["PG_HOST"], port=ENV["PG_PORT"], database=ENV["PG_DATABASE"],
            user=ENV["PG_USER"], password=ENV["PG_PASSWORD"], sslmode=ENV["PG_SSLMODE"]
        )
    
    def find_all_run_ids(self) -> List[str]:
        paths = self.lake.list_paths(self.base_path)
        run_ids = set()
        for p in paths:
            match = re.search(r"run_id=([^/]+)", p)
            if match:
                run_ids.add(match.group(1))
        return sorted(run_ids)
    
    def list_all_parts(self) -> List[str]:
        return self.lake.list_paths(self.base_path)
    
    def list_parts(self, run_id: str) -> List[str]:
        paths = self.lake.list_paths(self.base_path)
        return [p for p in paths if f"run_id={run_id}" in p]
    
    def extract_metadata(self, path: str) -> Tuple[str, str]:
        run_match = re.search(r"run_id=([^/]+)", path)
        date_match = re.search(r"ingestion_date=(\d{4}-\d{2}-\d{2})", path)
        run_id = run_match.group(1) if run_match else "unknown"
        ing_date = date_match.group(1) if date_match else datetime.now().strftime("%Y-%m-%d")
        return run_id, ing_date
    
    def prepare_copy_data(self, records: List[Dict], source_file: str) -> io.StringIO:
        """Prepara dados para COPY. Cria entry_id determinístico via MD5."""
        import hashlib
        
        buffer = io.StringIO()
        now = datetime.now(timezone.utc).isoformat()
        run_id, ing_date = self.extract_metadata(source_file)
        
        for rec in records:
            entry_date = rec.get("date")
            if not entry_date:
                continue
            
            # Chave determinística: MD5 de campos que identificam a entrada
            # date + idMember/idProspect/idEmployee + idBranch + device + entryAction
            key_parts = [
                str(entry_date),
                str(rec.get("idMember") or ""),
                str(rec.get("idProspect") or ""),
                str(rec.get("idEmployee") or ""),
                str(rec.get("idBranch") or ""),
                str(rec.get("device") or ""),
                str(rec.get("entryAction") or ""),
            ]
            key_str = "|".join(key_parts)
            
            # MD5 -> primeiros 15 dígitos como BIGINT
            md5_hash = hashlib.md5(key_str.encode()).hexdigest()
            entry_id = int(md5_hash[:15], 16) % (10**15)
            
            raw_json = json.dumps(rec, ensure_ascii=False, default=str)
            raw_json = raw_json.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
            
            buffer.write(f"{entry_id}\t{entry_date}\t{raw_json}\t{source_file}\t{run_id}\t{ing_date}\t{now}\n")
        
        buffer.seek(0)
        return buffer
    
    def load_batch_with_copy(self, conn, records: List[Dict], source_file: str) -> int:
        if not records:
            return 0
        
        cur = conn.cursor()
        try:
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS tmp_entries (
                    entry_id BIGINT,
                    entry_date TIMESTAMPTZ,
                    raw_data JSONB,
                    _source_file TEXT,
                    run_id TEXT,
                    ingestion_date DATE,
                    _loaded_at TIMESTAMPTZ
                ) ON COMMIT DROP
            """)
            cur.execute("TRUNCATE tmp_entries")
            
            buffer = self.prepare_copy_data(records, source_file)
            cur.copy_from(buffer, 'tmp_entries', sep='\t',
                         columns=('entry_id', 'entry_date', 'raw_data', '_source_file', 'run_id', 'ingestion_date', '_loaded_at'))
            
            # INSERT com ON CONFLICT (tabela particionada)
            cur.execute("""
                INSERT INTO stg_evo.entries_raw (entry_id, entry_date, raw_data, _source_file, run_id, ingestion_date, _loaded_at)
                SELECT entry_id, entry_date, raw_data, _source_file, run_id, ingestion_date, _loaded_at
                FROM tmp_entries
                ON CONFLICT (entry_id, entry_date) DO UPDATE SET
                    raw_data = EXCLUDED.raw_data,
                    _source_file = EXCLUDED._source_file,
                    run_id = EXCLUDED.run_id
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
        return part_path, count, elapsed
    
    def run(self, run_id: Optional[str] = None, all_runs: bool = False) -> Dict[str, Any]:
        logger.info("="*60)
        logger.info("EVO ENTRIES - LOADER OTIMIZADO (COPY)")
        logger.info("="*60)
        
        if all_runs:
            run_ids = self.find_all_run_ids()
            logger.info(f"Modo: TODOS os run_ids ({len(run_ids)})")
            parts = self.list_all_parts()
        elif run_id:
            logger.info(f"Run ID: {run_id}")
            parts = self.list_parts(run_id)
        else:
            run_ids = self.find_all_run_ids()
            if not run_ids:
                return {"error": "No run_id found"}
            run_id = run_ids[-1]
            logger.info(f"Run ID mais recente: {run_id}")
            parts = self.list_parts(run_id)
        
        logger.info(f"Parts: {len(parts)}")
        
        if not parts:
            return {"error": "No parts found"}
        
        total_records = 0
        processed = 0
        errors = 0
        start_time = datetime.now()
        
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(self.process_part, p): p for p in parts}
            for future in as_completed(futures):
                try:
                    path, count, elapsed = future.result()
                    total_records += count
                    processed += 1
                    if processed % 100 == 0 or processed == len(parts):
                        logger.info(f"  [{processed}/{len(parts)}] {total_records:,} entries carregados")
                except Exception as e:
                    errors += 1
                    logger.error(f"Erro: {e}")
        
        total_elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info("="*60)
        logger.info(f"Total: {total_records:,} em {total_elapsed:.1f}s ({total_records/total_elapsed:,.0f}/s)")
        
        return {
            "run_id": run_id if not all_runs else "ALL",
            "parts": processed,
            "records": total_records,
            "seconds": total_elapsed,
            "errors": errors
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id")
    parser.add_argument("--all-runs", action="store_true")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    
    result = FastEntriesLoader(workers=args.workers).run(run_id=args.run_id, all_runs=args.all_runs)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
