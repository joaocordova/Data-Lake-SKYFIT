# -*- coding: utf-8 -*-
"""
Loader OTIMIZADO: EVO Prospects Bronze → STG (PostgreSQL)

Usa COPY para performance máxima (50-100x mais rápido).

Uso:
    cd C:/skyfit-datalake/evo_prospects
    python src/loaders/load_evo_prospects_stg_fast.py --workers 4
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


def load_env():
    """Carrega .env usando python-dotenv."""
    env_paths = [
        PROJECT_ROOT / "config" / ".env",
        PROJECT_ROOT / ".env",
        PROJECT_ROOT.parent / "config" / ".env",
        Path(r"C:\skyfit-datalake\config\.env"),
    ]
    
    try:
        from dotenv import load_dotenv
        for env_path in env_paths:
            if env_path.exists():
                load_dotenv(env_path, override=True)
                print(f"[ENV] Carregado: {env_path}")
                return True
    except ImportError:
        print("[ENV] ERRO: python-dotenv não instalado")
        print("[ENV] Execute: pip install python-dotenv")
        sys.exit(1)
    
    print("[ENV] AVISO: .env não encontrado")
    return False


load_env()

# Configuração - aceita ambos os nomes de variáveis
ENV = {
    # Azure - aceita tanto os nomes antigos quanto os novos
    "AZURE_STORAGE_ACCOUNT_NAME": os.environ.get("AZURE_STORAGE_ACCOUNT") or os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
    "AZURE_STORAGE_ACCOUNT_KEY": os.environ.get("AZURE_STORAGE_KEY") or os.environ.get("AZURE_STORAGE_ACCOUNT_KEY"),
    "AZURE_CONTAINER_NAME": os.environ.get("ADLS_CONTAINER") or os.environ.get("AZURE_CONTAINER_NAME", "datalake"),
    
    # PostgreSQL
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
    if len(ENV.get("AZURE_STORAGE_ACCOUNT_KEY", "")) < 50:
        errors.append(f"AZURE_STORAGE_KEY parece truncada (len={len(ENV.get('AZURE_STORAGE_ACCOUNT_KEY', ''))})")
    if not ENV.get("PG_HOST"):
        errors.append("PG_HOST não definida")
    
    if errors:
        print("[ENV] ERROS:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    
    print(f"[ENV] Azure Account: {ENV['AZURE_STORAGE_ACCOUNT_NAME']}")
    print(f"[ENV] Azure Container: {ENV['AZURE_CONTAINER_NAME']}")
    print(f"[ENV] Azure Key length: {len(ENV['AZURE_STORAGE_ACCOUNT_KEY'])}")

validate_env()

# Logging
log_file = LOG_DIR / f"load_evo_prospects_fast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)
logger.info(f"Log: {log_file}")

# Reduz verbosidade do SDK da Azure e requisições HTTP
for noisy_logger in [
    "azure",
    "azure.core",
    "azure.identity",
    "azure.storage",
    "azure.storage.filedatalake",
    "azure.core.pipeline.policies.http_logging_policy",
]:
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)


# Silencia logs verbosos do Azure SDK no terminal
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


class SimpleLakeClient:
    """Cliente Azure Data Lake."""
    
    def __init__(self):
        from azure.storage.filedatalake import DataLakeServiceClient
        
        self.account_name = ENV["AZURE_STORAGE_ACCOUNT_NAME"]
        self.account_key = ENV["AZURE_STORAGE_ACCOUNT_KEY"]
        self.container_name = ENV["AZURE_CONTAINER_NAME"]
        
        self.service_client = DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=self.account_key
        )
        self.fs_client = self.service_client.get_file_system_client(self.container_name)
    
    def list_paths(self, path: str) -> List[str]:
        """Lista arquivos em um path."""
        paths = []
        try:
            for item in self.fs_client.get_paths(path=path, recursive=True):
                if not item.is_directory:
                    paths.append(item.name)
        except Exception as e:
            logger.error(f"Erro listando {path}: {e}")
            raise
        return sorted(paths)
    
    def read_gzip_jsonl(self, path: str) -> List[Dict]:
        """Lê arquivo JSONL comprimido."""
        file_client = self.fs_client.get_file_client(path)
        download = file_client.download_file()
        compressed = download.readall()
        
        decompressed = gzip.decompress(compressed)
        lines = decompressed.decode('utf-8').strip().split('\n')
        
        return [json.loads(line) for line in lines if line.strip()]


class FastProspectsLoader:
    """Loader otimizado usando COPY."""
    
    def __init__(self, workers: int = 2):
        self.lake = SimpleLakeClient()
        self.workers = workers
        self.base_path = "bronze/evo/entity=prospects"
    
    def get_pg_connection(self):
        """Cria nova conexão PostgreSQL."""
        return psycopg2.connect(
            host=ENV["PG_HOST"],
            port=ENV["PG_PORT"],
            database=ENV["PG_DATABASE"],
            user=ENV["PG_USER"],
            password=ENV["PG_PASSWORD"],
            sslmode=ENV["PG_SSLMODE"],
        )
    
    def find_latest_run_id(self) -> Optional[str]:
        """Encontra o run_id mais recente."""
        paths = self.lake.list_paths(self.base_path)
        
        run_ids = set()
        for p in paths:
            if "run_id=" in p:
                parts = p.split("/")
                for part in parts:
                    if part.startswith("run_id="):
                        run_ids.add(part.replace("run_id=", ""))
        
        if not run_ids:
            return None
        
        return sorted(run_ids)[-1]
    
    def list_parts(self, run_id: str) -> List[str]:
        """Lista todos os parts de um run_id."""
        paths = self.lake.list_paths(self.base_path)
        return [p for p in paths if f"run_id={run_id}" in p and p.endswith('.jsonl.gz')]
    
    def prepare_copy_data(self, records: List[Dict], source_file: str) -> io.StringIO:
        """Prepara dados para COPY em formato TSV."""
        buffer = io.StringIO()
        now = datetime.now(timezone.utc).isoformat()

        # Extrai metadados do path: ingestion_date e run_id
        m_run = re.search(r"run_id=([^/]+)", source_file)
        m_dt = re.search(r"ingestion_date=(\d{4}-\d{2}-\d{2})", source_file)

        if not m_run or not m_dt:
            raise ValueError(f"Não consegui extrair run_id/ingestion_date do path: {source_file}")

        run_id = m_run.group(1)
        ingestion_date = m_dt.group(1)  # YYYY-MM-DD

        for line_no, record in enumerate(records, start=1):
            prospect_id = record.get("idProspect")
            if prospect_id is None:
                continue

            # Escape para TSV
            raw_json = json.dumps(record, ensure_ascii=False)
            raw_json = (
                raw_json
                .replace('\\', '\\\\')
                .replace('\t', '\\t')
                .replace('\n', '\\n')
                .replace('\r', '\\r')
            )

            # 7 colunas para o COPY
            line = (
                f"{prospect_id}\t{raw_json}\t{source_file}\t"
                f"{line_no}\t{run_id}\t{ingestion_date}\t{now}\n"
            )
            buffer.write(line)

        buffer.seek(0)
        return buffer


    def load_batch_with_copy(self, conn, records: List[Dict], source_file: str) -> int:
        """Carrega batch usando COPY + tabela temporária."""
        if not records:
            return 0

        cur = conn.cursor()

        try:
            # 1) Tabela temporária com TODOS os campos NOT NULL da tabela final
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS tmp_prospects (
                    prospect_id BIGINT,
                    raw_data JSONB,
                    _source_file TEXT,
                    source_line_no INTEGER,
                    run_id TEXT,
                    ingestion_date DATE,
                    _loaded_at TIMESTAMPTZ
                ) ON COMMIT DROP
            """)

            # 2) Limpa temp
            cur.execute("TRUNCATE tmp_prospects")

            # 3) COPY
            buffer = self.prepare_copy_data(records, source_file)
            cur.copy_from(
                buffer,
                'tmp_prospects',
                sep='\t',
                columns=('prospect_id', 'raw_data', '_source_file', 'source_line_no', 'run_id', 'ingestion_date', '_loaded_at')
            )

            # 4) UPSERT na tabela final preenchendo também os metadados
            cur.execute("""
                INSERT INTO stg_evo.prospects_raw
                    (prospect_id, raw_data, _source_file, source_line_no, run_id, ingestion_date, _loaded_at, _updated_at)
                SELECT
                    prospect_id, raw_data, _source_file, source_line_no, run_id, ingestion_date, _loaded_at, _loaded_at
                FROM tmp_prospects
                ON CONFLICT (prospect_id) DO UPDATE SET
                    raw_data       = EXCLUDED.raw_data,
                    _source_file   = EXCLUDED._source_file,
                    source_line_no = EXCLUDED.source_line_no,
                    run_id         = EXCLUDED.run_id,
                    ingestion_date = EXCLUDED.ingestion_date,
                    _updated_at    = EXCLUDED._loaded_at
            """)

            count = cur.rowcount
            conn.commit()
            return count

        except Exception as e:
            conn.rollback()
            raise e
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
    
    def run(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        """Executa o load completo."""
        logger.info("="*60)
        logger.info("EVO PROSPECTS - LOADER OTIMIZADO (COPY)")
        logger.info("="*60)
        
        if not run_id:
            run_id = self.find_latest_run_id()
        
        if not run_id:
            logger.error("Nenhum run_id encontrado no Data Lake")
            return {"error": "No run_id found"}
        
        logger.info(f"Run ID: {run_id}")
        
        parts = self.list_parts(run_id)
        logger.info(f"Parts encontrados: {len(parts)}")
        
        if not parts:
            return {"error": "No parts found", "run_id": run_id}
        
        total_records = 0
        processed = 0
        start_time = datetime.now()
        
        if self.workers > 1:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = {executor.submit(self.process_part, part): part for part in parts}
                
                for future in as_completed(futures):
                    part_path = futures[future]
                    try:
                        path, count, elapsed = future.result()
                        total_records += count
                        processed += 1
                        
                        part_name = Path(path).name
                        rate = count / elapsed if elapsed > 0 else 0
                        logger.info(f"  [{processed}/{len(parts)}] {part_name}: {count:,} registros em {elapsed:.1f}s ({rate:,.0f} rec/s)")
                        
                    except Exception as e:
                        logger.error(f"Erro em {part_path}: {e}")
        else:
            for part in parts:
                try:
                    path, count, elapsed = self.process_part(part)
                    total_records += count
                    processed += 1
                    
                    part_name = Path(path).name
                    rate = count / elapsed if elapsed > 0 else 0
                    logger.info(f"  [{processed}/{len(parts)}] {part_name}: {count:,} registros em {elapsed:.1f}s ({rate:,.0f} rec/s)")
                    
                except Exception as e:
                    logger.error(f"Erro em {part}: {e}")
        
        total_elapsed = (datetime.now() - start_time).total_seconds()
        avg_rate = total_records / total_elapsed if total_elapsed > 0 else 0
        
        result = {
            "run_id": run_id,
            "parts_processed": processed,
            "total_records": total_records,
            "total_time_seconds": total_elapsed,
            "avg_records_per_second": avg_rate,
            "workers": self.workers,
        }
        
        logger.info("="*60)
        logger.info("RESULTADO")
        logger.info("="*60)
        logger.info(f"Parts processados: {processed}/{len(parts)}")
        logger.info(f"Total de registros: {total_records:,}")
        logger.info(f"Tempo total: {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
        logger.info(f"Taxa média: {avg_rate:,.0f} registros/segundo")
        
        return result


def main():
    parser = argparse.ArgumentParser(description="Loader otimizado para EVO Prospects")
    parser.add_argument("--run-id", help="Run ID específico")
    parser.add_argument("--workers", type=int, default=2, help="Workers paralelos (default: 2)")
    args = parser.parse_args()
    
    loader = FastProspectsLoader(workers=args.workers)
    result = loader.run(run_id=args.run_id)
    
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
