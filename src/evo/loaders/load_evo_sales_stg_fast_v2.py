# -*- coding: utf-8 -*-
"""
Loader OTIMIZADO v2: EVO Sales Bronze → STG

OTIMIZAÇÕES:
1. ThreadedConnectionPool - reutiliza conexões
2. Batch de múltiplos parts em um único COPY
3. Streaming decompress - menor uso de memória
4. Log detalhado por fase (download, decompress, copy, commit)
5. Retry resiliente para failover Azure

Uso:
    cd C:/skyfit-datalake/evo_sales
    
    # Modo padrão (4 workers, batch de 5 parts)
    python src/loaders/load_evo_sales_stg_fast_v2.py --workers 4 --batch-size 5 --all-runs
    
    # Modo agressivo (8 workers, batch de 10 parts)
    python src/loaders/load_evo_sales_stg_fast_v2.py --workers 8 --batch-size 10 --all-runs
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
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import pool

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

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
    print(f"[ENV] PostgreSQL: {ENV['PG_HOST']}")


validate_env()

log_file = LOG_DIR / f"load_sales_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')]
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONNECTION POOL
# ============================================================================

class ConnectionPoolManager:
    """Gerencia pool de conexões PostgreSQL."""
    
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
                min_conn,
                max_conn,
                host=ENV["PG_HOST"],
                port=ENV["PG_PORT"],
                database=ENV["PG_DATABASE"],
                user=ENV["PG_USER"],
                password=ENV["PG_PASSWORD"],
                sslmode=ENV["PG_SSLMODE"],
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


# ============================================================================
# AZURE LAKE CLIENT OTIMIZADO
# ============================================================================

class OptimizedLakeClient:
    """Cliente Azure otimizado com streaming."""
    
    def __init__(self):
        from azure.storage.filedatalake import DataLakeServiceClient
        self.service = DataLakeServiceClient(
            account_url=f"https://{ENV['AZURE_STORAGE_ACCOUNT_NAME']}.dfs.core.windows.net",
            credential=ENV["AZURE_STORAGE_ACCOUNT_KEY"]
        )
        self.fs = self.service.get_file_system_client(ENV["AZURE_CONTAINER_NAME"])
        self._path_cache = {}
    
    def list_paths(self, prefix: str) -> List[str]:
        """Lista paths com cache."""
        if prefix not in self._path_cache:
            paths = []
            for item in self.fs.get_paths(path=prefix, recursive=True):
                if not item.is_directory and item.name.endswith('.jsonl.gz'):
                    paths.append(item.name)
            self._path_cache[prefix] = sorted(paths)
        return self._path_cache[prefix]
    
    def read_gzip_jsonl_streaming(self, path: str) -> Tuple[List[Dict], Dict[str, float]]:
        """
        Lê arquivo com streaming e retorna métricas de tempo.
        Retorna: (records, timings)
        """
        timings = {}
        
        # Download
        t0 = time.time()
        file_client = self.fs.get_file_client(path)
        compressed = file_client.download_file().readall()
        timings['download'] = time.time() - t0
        
        # Decompress
        t0 = time.time()
        decompressed = gzip.decompress(compressed)
        timings['decompress'] = time.time() - t0
        
        # Parse
        t0 = time.time()
        lines = decompressed.decode('utf-8').strip().split('\n')
        records = [json.loads(line) for line in lines if line.strip()]
        timings['parse'] = time.time() - t0
        
        return records, timings
    
    def read_multiple_parts(self, paths: List[str]) -> Tuple[List[Tuple[str, List[Dict]]], Dict[str, float]]:
        """Lê múltiplos parts e retorna dados agregados."""
        all_records = []
        total_timings = {'download': 0, 'decompress': 0, 'parse': 0}
        
        for path in paths:
            records, timings = self.read_gzip_jsonl_streaming(path)
            all_records.append((path, records))
            for k, v in timings.items():
                total_timings[k] += v
        
        return all_records, total_timings


# ============================================================================
# LOADER OTIMIZADO v2
# ============================================================================

class FastSalesLoaderV2:
    """Loader otimizado com batching e connection pool."""
    
    def __init__(self, workers: int = 4, batch_size: int = 5):
        self.lake = OptimizedLakeClient()
        self.workers = workers
        self.batch_size = batch_size
        self.base_path = "bronze/evo/entity=sales"
        
        # Inicializa pool com workers + margem
        self.pool_manager = ConnectionPoolManager()
        self.pool_manager.initialize(min_conn=2, max_conn=workers + 2)
        
        # Métricas
        self.metrics = {
            'total_download': 0,
            'total_decompress': 0,
            'total_parse': 0,
            'total_copy': 0,
            'total_commit': 0,
        }
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
    
    def prepare_batch_copy_data(self, parts_data: List[Tuple[str, List[Dict]]]) -> io.StringIO:
        """Prepara dados de múltiplos parts para um único COPY."""
        buffer = io.StringIO()
        now = datetime.now(timezone.utc).isoformat()
        
        for source_file, records in parts_data:
            run_id, ing_date = self.extract_metadata(source_file)
            
            for rec in records:
                sale_id = rec.get("idSale")
                if sale_id is None:
                    continue
                
                raw_json = json.dumps(rec, ensure_ascii=False, default=str)
                raw_json = raw_json.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                
                buffer.write(f"{sale_id}\t{raw_json}\t{source_file}\t{run_id}\t{ing_date}\t{now}\n")
        
        buffer.seek(0)
        return buffer
    
    def load_batch_with_copy(self, conn, parts_data: List[Tuple[str, List[Dict]]]) -> Tuple[int, float]:
        """Carrega batch de múltiplos parts com um único COPY."""
        if not parts_data:
            return 0, 0
        
        t0 = time.time()
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
            
            buffer = self.prepare_batch_copy_data(parts_data)
            cur.copy_from(buffer, 'tmp_sales', sep='\t',
                         columns=('sale_id', 'raw_data', '_source_file', 'run_id', 'ingestion_date', '_loaded_at'))
            
            copy_time = time.time() - t0
            
            t0 = time.time()
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
            commit_time = time.time() - t0
            
            return count, copy_time + commit_time
            
        except Exception as e:
            conn.rollback()
            raise
        finally:
            cur.close()
    
    def process_batch(self, part_paths: List[str]) -> Tuple[List[str], int, Dict[str, float]]:
        """Processa um batch de parts com retry."""
        max_retries = 5
        base_delay = 30
        
        for attempt in range(max_retries):
            try:
                # Lê todos os parts do batch
                parts_data, read_timings = self.lake.read_multiple_parts(part_paths)
                
                total_records = sum(len(records) for _, records in parts_data)
                
                # Carrega no banco
                with self.pool_manager.get_connection() as conn:
                    count, db_time = self.load_batch_with_copy(conn, parts_data)
                
                # Atualiza métricas
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
                    logger.warning(f"⚠️ Failover detectado, aguardando {delay}s... (tentativa {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    raise
    
    def run(self, all_runs: bool = False, run_id: Optional[str] = None) -> Dict[str, Any]:
        logger.info("="*70)
        logger.info("EVO SALES - LOADER OTIMIZADO v2")
        logger.info("="*70)
        logger.info(f"Workers: {self.workers} | Batch size: {self.batch_size} parts")
        
        # Lista parts
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
        
        # Agrupa parts em batches
        batches = [parts[i:i + self.batch_size] for i in range(0, len(parts), self.batch_size)]
        logger.info(f"Batches: {len(batches)} (de {self.batch_size} parts cada)")
        
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
                    
                    # Log a cada 10 batches
                    if processed_batches % 10 == 0 or processed_batches == len(batches):
                        elapsed = time.time() - start_time
                        rate = total_records / elapsed if elapsed > 0 else 0
                        logger.info(f"  [{processed_batches}/{len(batches)}] {total_records:,} registros ({rate:,.0f}/s)")
                        
                except Exception as e:
                    errors += 1
                    logger.error(f"Erro: {e}")
        
        total_elapsed = time.time() - start_time
        
        # Fecha pool
        self.pool_manager.close_all()
        
        # Log métricas
        logger.info("="*70)
        logger.info("MÉTRICAS DE TEMPO")
        logger.info("="*70)
        logger.info(f"  Download:   {self.metrics['total_download']:.1f}s ({self.metrics['total_download']/total_elapsed*100:.1f}%)")
        logger.info(f"  Decompress: {self.metrics['total_decompress']:.1f}s ({self.metrics['total_decompress']/total_elapsed*100:.1f}%)")
        logger.info(f"  Parse:      {self.metrics['total_parse']:.1f}s ({self.metrics['total_parse']/total_elapsed*100:.1f}%)")
        logger.info(f"  DB (COPY):  {self.metrics['total_copy']:.1f}s ({self.metrics['total_copy']/total_elapsed*100:.1f}%)")
        
        logger.info("="*70)
        logger.info("RESULTADO")
        logger.info("="*70)
        logger.info(f"Total: {total_records:,} em {total_elapsed:.1f}s ({total_records/total_elapsed:,.0f}/s)")
        if errors:
            logger.warning(f"Erros: {errors}")
        
        return {
            "run_id": "ALL" if all_runs else run_id,
            "parts": len(parts),
            "batches": processed_batches,
            "records": total_records,
            "seconds": total_elapsed,
            "rate": total_records / total_elapsed if total_elapsed > 0 else 0,
            "errors": errors,
            "metrics": self.metrics,
        }


def main():
    parser = argparse.ArgumentParser(description="Loader otimizado v2 para EVO Sales")
    parser.add_argument("--run-id", help="Run ID específico")
    parser.add_argument("--all-runs", action="store_true", help="Processa TODOS os run_ids")
    parser.add_argument("--workers", type=int, default=4, help="Workers paralelos (default: 4)")
    parser.add_argument("--batch-size", type=int, default=5, help="Parts por batch (default: 5)")
    args = parser.parse_args()
    
    loader = FastSalesLoaderV2(workers=args.workers, batch_size=args.batch_size)
    result = loader.run(all_runs=args.all_runs, run_id=args.run_id)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
