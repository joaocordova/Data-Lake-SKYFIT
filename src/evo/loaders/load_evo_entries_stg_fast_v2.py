# -*- coding: utf-8 -*-
"""
Loader OTIMIZADO v2: EVO Entries Bronze → STG

OTIMIZADO PARA ALTO VOLUME (~110M registros):
1. ThreadedConnectionPool
2. Batch de múltiplos parts
3. Streaming decompress
4. Particionamento por ano

Uso:
    cd C:/skyfit-datalake/evo_entries
    python src/loaders/load_evo_entries_stg_fast_v2.py --workers 8 --batch-size 10 --all-runs
"""
import argparse
import gzip
import hashlib
import io
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import pool

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.getLogger("azure").setLevel(logging.WARNING)


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
    key_len = len(ENV.get("AZURE_STORAGE_ACCOUNT_KEY", ""))
    if key_len < 50 or not ENV.get("PG_HOST"):
        print("[ENV] Configuração incompleta")
        sys.exit(1)
    print(f"[ENV] Azure: {ENV['AZURE_STORAGE_ACCOUNT_NAME']}")
    print(f"[ENV] PostgreSQL: {ENV['PG_HOST']}")


validate_env()

log_file = LOG_DIR / f"load_entries_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class ConnectionPoolManager:
    _instance = None
    _lock = Lock()
    
    def __new__(cls, min_conn: int = 2, max_conn: int = 10):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._pool = None
                cls._instance._initialized = False
            return cls._instance
    
    def initialize(self, min_conn: int = 2, max_conn: int = 10):
        if not self._initialized:
            self._pool = pool.ThreadedConnectionPool(
                min_conn, max_conn,
                host=ENV["PG_HOST"], port=ENV["PG_PORT"], database=ENV["PG_DATABASE"],
                user=ENV["PG_USER"], password=ENV["PG_PASSWORD"], sslmode=ENV["PG_SSLMODE"],
            )
            self._initialized = True
            logger.info(f"[POOL] Inicializado: {min_conn}-{max_conn} conexões")
    
    @contextmanager
    def get_connection(self):
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)
    
    def close_all(self):
        if self._pool:
            self._pool.closeall()
            self._initialized = False


class OptimizedLakeClient:
    def __init__(self):
        from azure.storage.filedatalake import DataLakeServiceClient
        self.service = DataLakeServiceClient(
            account_url=f"https://{ENV['AZURE_STORAGE_ACCOUNT_NAME']}.dfs.core.windows.net",
            credential=ENV["AZURE_STORAGE_ACCOUNT_KEY"]
        )
        self.fs = self.service.get_file_system_client(ENV["AZURE_CONTAINER_NAME"])
        self._path_cache = {}
    
    def list_paths(self, prefix: str) -> List[str]:
        if prefix not in self._path_cache:
            paths = []
            for item in self.fs.get_paths(path=prefix, recursive=True):
                if not item.is_directory and item.name.endswith('.jsonl.gz'):
                    paths.append(item.name)
            self._path_cache[prefix] = sorted(paths)
        return self._path_cache[prefix]
    
    def read_multiple_parts(self, paths: List[str]) -> Tuple[List[Tuple[str, List[Dict]]], Dict[str, float]]:
        all_records = []
        total_timings = {'download': 0, 'decompress': 0, 'parse': 0}
        
        for path in paths:
            t0 = time.time()
            file_client = self.fs.get_file_client(path)
            compressed = file_client.download_file().readall()
            total_timings['download'] += time.time() - t0
            
            t0 = time.time()
            decompressed = gzip.decompress(compressed)
            total_timings['decompress'] += time.time() - t0
            
            t0 = time.time()
            lines = decompressed.decode('utf-8').strip().split('\n')
            records = [json.loads(line) for line in lines if line.strip()]
            total_timings['parse'] += time.time() - t0
            
            all_records.append((path, records))
        
        return all_records, total_timings


class FastEntriesLoaderV2:
    def __init__(self, workers: int = 8, batch_size: int = 10):
        self.lake = OptimizedLakeClient()
        self.workers = workers
        self.batch_size = batch_size
        self.base_path = "bronze/evo/entity=entries"
        
        self.pool_manager = ConnectionPoolManager()
        self.pool_manager.initialize(min_conn=2, max_conn=workers + 2)
        
        self.metrics = {'total_download': 0, 'total_decompress': 0, 'total_parse': 0, 'total_copy': 0}
        self.metrics_lock = Lock()
    
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
    
    def extract_metadata(self, path: str) -> Tuple[str, str]:
        run_match = re.search(r"run_id=([^/]+)", path)
        date_match = re.search(r"ingestion_date=(\d{4}-\d{2}-\d{2})", path)
        run_id = run_match.group(1) if run_match else "unknown"
        ing_date = date_match.group(1) if date_match else datetime.now().strftime("%Y-%m-%d")
        return run_id, ing_date
    
    def generate_entry_id(self, rec: Dict) -> int:
        """Gera entry_id determinístico via MD5."""
        key_parts = [
            str(rec.get("date", "")),
            str(rec.get("idMember") or ""),
            str(rec.get("idProspect") or ""),
            str(rec.get("idEmployee") or ""),
            str(rec.get("idBranch") or ""),
            str(rec.get("device") or ""),
            str(rec.get("entryAction") or ""),
        ]
        key_str = "|".join(key_parts)
        md5_hash = hashlib.md5(key_str.encode()).hexdigest()
        return int(md5_hash[:15], 16) % (10**15)
    
    def prepare_batch_copy_data(self, parts_data: List[Tuple[str, List[Dict]]]) -> io.StringIO:
        buffer = io.StringIO()
        now = datetime.now(timezone.utc).isoformat()
        
        for source_file, records in parts_data:
            run_id, ing_date = self.extract_metadata(source_file)
            
            for rec in records:
                entry_date = rec.get("date")
                if not entry_date:
                    continue
                
                entry_id = self.generate_entry_id(rec)
                
                raw_json = json.dumps(rec, ensure_ascii=False, default=str)
                raw_json = raw_json.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                
                buffer.write(f"{entry_id}\t{entry_date}\t{raw_json}\t{source_file}\t{run_id}\t{ing_date}\t{now}\n")
        
        buffer.seek(0)
        return buffer
    
    def load_batch_with_copy(self, conn, parts_data: List[Tuple[str, List[Dict]]]) -> Tuple[int, float]:
        if not parts_data:
            return 0, 0
        
        t0 = time.time()
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
            
            buffer = self.prepare_batch_copy_data(parts_data)
            cur.copy_from(buffer, 'tmp_entries', sep='\t',
                         columns=('entry_id', 'entry_date', 'raw_data', '_source_file', 'run_id', 'ingestion_date', '_loaded_at'))
            
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
            return count, time.time() - t0
            
        except Exception as e:
            conn.rollback()
            raise
        finally:
            cur.close()
    
    def process_batch(self, part_paths: List[str]) -> Tuple[List[str], int, Dict[str, float]]:
        max_retries = 5
        base_delay = 30
        
        for attempt in range(max_retries):
            try:
                parts_data, read_timings = self.lake.read_multiple_parts(part_paths)
                
                with self.pool_manager.get_connection() as conn:
                    count, db_time = self.load_batch_with_copy(conn, parts_data)
                
                with self.metrics_lock:
                    self.metrics['total_download'] += read_timings['download']
                    self.metrics['total_decompress'] += read_timings['decompress']
                    self.metrics['total_parse'] += read_timings['parse']
                    self.metrics['total_copy'] += db_time
                
                return part_paths, count, {**read_timings, 'db': db_time}
                
            except Exception as e:
                error_msg = str(e).lower()
                is_failover = any(x in error_msg for x in [
                    'read-only', 'readonly', 'connection already closed',
                    'adminshutdown', 'server closed', 'terminating connection'
                ])
                
                if is_failover and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"⚠️ Failover detectado, aguardando {delay}s...")
                    time.sleep(delay)
                    continue
                else:
                    raise
    
    def run(self, all_runs: bool = False, run_id: Optional[str] = None) -> Dict[str, Any]:
        logger.info("="*70)
        logger.info("EVO ENTRIES - LOADER OTIMIZADO v2 (ALTO VOLUME)")
        logger.info("="*70)
        logger.info(f"Workers: {self.workers} | Batch size: {self.batch_size} parts")
        
        if all_runs:
            run_ids = self.find_all_run_ids()
            logger.info(f"Modo: TODOS os run_ids ({len(run_ids)})")
            parts = self.list_all_parts()
        else:
            run_ids = self.find_all_run_ids()
            run_id = run_id or (run_ids[-1] if run_ids else None)
            if not run_id:
                return {"error": "No run_id found"}
            parts = [p for p in self.list_all_parts() if f"run_id={run_id}" in p]
        
        logger.info(f"Parts: {len(parts)}")
        
        if not parts:
            return {"error": "No parts found"}
        
        batches = [parts[i:i + self.batch_size] for i in range(0, len(parts), self.batch_size)]
        logger.info(f"Batches: {len(batches)}")
        
        total_records = 0
        processed_batches = 0
        errors = 0
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(self.process_batch, batch): batch for batch in batches}
            
            for future in as_completed(futures):
                try:
                    paths, count, timings = future.result()
                    total_records += count
                    processed_batches += 1
                    
                    if processed_batches % 50 == 0 or processed_batches == len(batches):
                        elapsed = time.time() - start_time
                        rate = total_records / elapsed if elapsed > 0 else 0
                        logger.info(f"  [{processed_batches}/{len(batches)}] {total_records:,} ({rate:,.0f}/s)")
                        
                except Exception as e:
                    errors += 1
                    logger.error(f"Erro: {e}")
        
        total_elapsed = time.time() - start_time
        self.pool_manager.close_all()
        
        logger.info("="*70)
        logger.info("MÉTRICAS DE TEMPO")
        logger.info("="*70)
        for k, v in self.metrics.items():
            pct = v / total_elapsed * 100 if total_elapsed > 0 else 0
            logger.info(f"  {k}: {v:.1f}s ({pct:.1f}%)")
        
        logger.info("="*70)
        logger.info(f"Total: {total_records:,} em {total_elapsed:.1f}s ({total_records/total_elapsed:,.0f}/s)")
        
        return {
            "run_id": "ALL" if all_runs else run_id,
            "parts": len(parts),
            "records": total_records,
            "seconds": total_elapsed,
            "rate": total_records / total_elapsed if total_elapsed > 0 else 0,
            "errors": errors,
            "metrics": self.metrics,
        }


def main():
    parser = argparse.ArgumentParser(description="Loader otimizado v2 para EVO Entries")
    parser.add_argument("--run-id", help="Run ID específico")
    parser.add_argument("--all-runs", action="store_true", help="Processa TODOS os run_ids")
    parser.add_argument("--workers", type=int, default=8, help="Workers paralelos")
    parser.add_argument("--batch-size", type=int, default=10, help="Parts por batch")
    args = parser.parse_args()
    
    loader = FastEntriesLoaderV2(workers=args.workers, batch_size=args.batch_size)
    result = loader.run(all_runs=args.all_runs, run_id=args.run_id)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
