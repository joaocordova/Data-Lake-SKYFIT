# -*- coding: utf-8 -*-
"""
Transformer: EVO Entries STG → CORE

Normaliza entries do JSONB para tabela tipada.
Otimizado para ALTO VOLUME (~110M registros).

Uso:
    cd C:/skyfit-datalake/evo_entries
    python src/transformers/normalize_evo_entries.py
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)


def load_env():
    env_paths = [
        PROJECT_ROOT / "config" / ".env",
        PROJECT_ROOT.parent / "config" / ".env",
        Path(r"C:\skyfit-datalake\config\.env"),
    ]
    try:
        from dotenv import load_dotenv
        for p in env_paths:
            if p.exists():
                load_dotenv(p, override=True)
                return True
    except ImportError:
        pass
    return False


load_env()

ENV = {
    "PG_HOST": os.environ.get("PG_HOST"),
    "PG_PORT": os.environ.get("PG_PORT", "5432"),
    "PG_DATABASE": os.environ.get("PG_DATABASE", "postgres"),
    "PG_USER": os.environ.get("PG_USER"),
    "PG_PASSWORD": os.environ.get("PG_PASSWORD"),
    "PG_SSLMODE": os.environ.get("PG_SSLMODE", "require"),
}

log_file = LOG_DIR / f"normalize_entries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class EntriesTransformer:
    """Transforma entries de JSONB para tabela tipada."""
    
    def __init__(self, batch_size: int = 100000):
        self.conn = psycopg2.connect(
            host=ENV["PG_HOST"],
            port=ENV["PG_PORT"],
            database=ENV["PG_DATABASE"],
            user=ENV["PG_USER"],
            password=ENV["PG_PASSWORD"],
            sslmode=ENV["PG_SSLMODE"],
        )
        self.conn.autocommit = False
        self.batch_size = batch_size
    
    def transform_batch(self, year: int = None) -> int:
        """Transforma entries em batches para melhor performance."""
        logger.info(f"Transformando entries{f' de {year}' if year else ''}...")
        
        # Filtro opcional por ano
        year_filter = f"AND entry_date >= '{year}-01-01' AND entry_date < '{year+1}-01-01'" if year else ""
        
        # Usa entry_id já calculado na STG (hash MD5 determinístico)
        sql = f"""
        INSERT INTO core.evo_entries (
            entry_id,
            entry_date,
            entry_date_turn,
            timezone,
            member_id,
            member_name,
            prospect_id,
            prospect_name,
            employee_id,
            employee_name,
            branch_id,
            entry_type,
            entry_action,
            device,
            block_reason,
            releases_by_id,
            migration_id,
            _source_run_id,
            _loaded_at
        )
        SELECT 
            entry_id,
            entry_date,
            (raw_data->>'dateTurn')::TIMESTAMPTZ,
            raw_data->>'timeZone',
            (raw_data->>'idMember')::BIGINT,
            raw_data->>'nameMember',
            (raw_data->>'idProspect')::BIGINT,
            raw_data->>'nameProspect',
            (raw_data->>'idEmployee')::BIGINT,
            raw_data->>'nameEmployee',
            (raw_data->>'idBranch')::BIGINT,
            raw_data->>'entryType',
            raw_data->>'entryAction',
            raw_data->>'device',
            raw_data->>'blockReason',
            (raw_data->>'releasesByID')::BIGINT,
            raw_data->>'idMigration',
            run_id,
            _loaded_at
        FROM stg_evo.entries_raw
        WHERE TRUE {year_filter}
        ON CONFLICT (entry_id, entry_date) DO UPDATE SET
            entry_date_turn = EXCLUDED.entry_date_turn,
            member_id = EXCLUDED.member_id,
            member_name = EXCLUDED.member_name,
            prospect_id = EXCLUDED.prospect_id,
            prospect_name = EXCLUDED.prospect_name,
            employee_id = EXCLUDED.employee_id,
            employee_name = EXCLUDED.employee_name,
            branch_id = EXCLUDED.branch_id,
            entry_type = EXCLUDED.entry_type,
            entry_action = EXCLUDED.entry_action,
            device = EXCLUDED.device,
            block_reason = EXCLUDED.block_reason,
            releases_by_id = EXCLUDED.releases_by_id,
            migration_id = EXCLUDED.migration_id,
            _source_run_id = EXCLUDED._source_run_id
        """
        
        with self.conn.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
        
        self.conn.commit()
        logger.info(f"  Entries transformados: {count:,}")
        return count
    
    def get_stats(self):
        stats = {}
        with self.conn.cursor() as cur:
            try:
                cur.execute("SELECT COUNT(*) FROM stg_evo.entries_raw")
                stats["stg"] = cur.fetchone()[0]
            except:
                stats["stg"] = 0
            
            try:
                cur.execute("SELECT COUNT(*) FROM core.evo_entries")
                stats["core"] = cur.fetchone()[0]
            except:
                stats["core"] = 0
        return stats
    
    def run(self, by_year: bool = False):
        logger.info("="*60)
        logger.info("EVO ENTRIES - TRANSFORMAÇÃO CORE")
        logger.info("="*60)
        
        start = datetime.now()
        total = 0
        
        if by_year:
            # Processa ano por ano para volumes muito grandes
            for year in range(2020, 2027):
                count = self.transform_batch(year=year)
                total += count
                if count > 0:
                    logger.info(f"  Ano {year}: {count:,} registros")
        else:
            total = self.transform_batch()
        
        elapsed = (datetime.now() - start).total_seconds()
        stats = self.get_stats()
        
        logger.info("="*60)
        logger.info("RESULTADO")
        logger.info("="*60)
        logger.info(f"Transformados: {total:,}")
        logger.info(f"STG: {stats['stg']:,} | CORE: {stats['core']:,}")
        logger.info(f"Tempo: {elapsed:.1f}s")
        
        return {"transformed": total, "elapsed": elapsed, "stats": stats}
    
    def close(self):
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--by-year", action="store_true", help="Processa ano por ano (recomendado para >10M)")
    args = parser.parse_args()
    
    transformer = EntriesTransformer()
    try:
        result = transformer.run(by_year=args.by_year)
        print(json.dumps(result, indent=2, default=str))
    finally:
        transformer.close()


if __name__ == "__main__":
    main()
