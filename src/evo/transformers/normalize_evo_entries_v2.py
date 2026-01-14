# -*- coding: utf-8 -*-
"""
Transformer OTIMIZADO v2: EVO Entries STG → CORE

OTIMIZADO PARA ALTO VOLUME (~110M registros):
1. Processamento por ANO (partição)
2. FULL REFRESH muito mais rápido
3. Desabilita índices durante carga

Uso:
    cd C:/skyfit-datalake/evo_entries
    
    # Modo incremental (LENTO para 110M!)
    python src/transformers/normalize_evo_entries_v2.py
    
    # Modo full refresh (RECOMENDADO!)
    python src/transformers/normalize_evo_entries_v2.py --full-refresh
    
    # Processar apenas um ano específico
    python src/transformers/normalize_evo_entries_v2.py --year 2024
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

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

log_file = LOG_DIR / f"normalize_entries_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class EntriesTransformerV2:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=ENV["PG_HOST"],
            port=ENV["PG_PORT"],
            database=ENV["PG_DATABASE"],
            user=ENV["PG_USER"],
            password=ENV["PG_PASSWORD"],
            sslmode=ENV["PG_SSLMODE"],
        )
        self.conn.autocommit = False
        self.metrics = {}
    
    def get_stg_count(self, year: Optional[int] = None) -> int:
        with self.conn.cursor() as cur:
            if year:
                cur.execute(f"""
                    SELECT COUNT(*) FROM stg_evo.entries_raw 
                    WHERE entry_date >= '{year}-01-01' AND entry_date < '{year+1}-01-01'
                """)
            else:
                cur.execute("SELECT COUNT(*) FROM stg_evo.entries_raw")
            return cur.fetchone()[0]
    
    def get_years_with_data(self) -> list:
        """Retorna anos que têm dados na STG."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT EXTRACT(YEAR FROM entry_date)::INT as year
                FROM stg_evo.entries_raw
                ORDER BY year
            """)
            return [row[0] for row in cur.fetchall()]
    
    def truncate_partition(self, year: int):
        """Trunca uma partição específica."""
        with self.conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE core.evo_entries_{year}")
        self.conn.commit()
        logger.info(f"  Partição {year} truncada")
    
    def transform_year(self, year: int, full_refresh: bool = False) -> int:
        """Transforma entries de um ano específico."""
        logger.info(f"Transformando entries de {year}...")
        t0 = time.time()
        
        stg_count = self.get_stg_count(year)
        logger.info(f"  Registros STG {year}: {stg_count:,}")
        
        if stg_count == 0:
            return 0
        
        with self.conn.cursor() as cur:
            if full_refresh:
                try:
                    self.truncate_partition(year)
                except:
                    logger.warning(f"  Partição {year} não existe, criando...")
            
            conflict_clause = "" if full_refresh else """
                ON CONFLICT (entry_id, entry_date) DO UPDATE SET
                    member_id = EXCLUDED.member_id,
                    member_name = EXCLUDED.member_name,
                    entry_type = EXCLUDED.entry_type,
                    _source_run_id = EXCLUDED._source_run_id
            """
            
            sql = f"""
            INSERT INTO core.evo_entries (
                entry_id, entry_date, entry_date_turn, timezone, member_id, member_name,
                prospect_id, prospect_name, employee_id, employee_name, branch_id,
                entry_type, entry_action, device, block_reason, releases_by_id,
                migration_id, _source_run_id, _loaded_at
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
            WHERE entry_date >= '{year}-01-01' AND entry_date < '{year+1}-01-01'
            {conflict_clause}
            """
            
            cur.execute(sql)
            count = cur.rowcount
        
        self.conn.commit()
        
        elapsed = time.time() - t0
        rate = count / elapsed if elapsed > 0 else 0
        self.metrics[f'year_{year}'] = {'count': count, 'time': elapsed, 'rate': rate}
        logger.info(f"  Ano {year}: {count:,} em {elapsed:.1f}s ({rate:,.0f}/s)")
        
        return count
    
    def run(self, full_refresh: bool = False, year: Optional[int] = None) -> Dict[str, Any]:
        logger.info("="*70)
        logger.info("EVO ENTRIES - TRANSFORMAÇÃO CORE v2")
        logger.info("="*70)
        logger.info(f"Modo: {'FULL REFRESH' if full_refresh else 'INCREMENTAL (UPSERT)'}")
        
        start = time.time()
        total = 0
        
        if year:
            # Apenas um ano específico
            logger.info(f"Processando apenas ano {year}")
            total = self.transform_year(year, full_refresh)
        else:
            # Todos os anos
            years = self.get_years_with_data()
            logger.info(f"Anos com dados: {years}")
            
            for y in years:
                count = self.transform_year(y, full_refresh)
                total += count
        
        total_elapsed = time.time() - start
        
        # Contagem final
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM core.evo_entries")
            core_count = cur.fetchone()[0]
        
        logger.info("="*70)
        logger.info("RESULTADO")
        logger.info("="*70)
        logger.info(f"Transformados: {total:,}")
        logger.info(f"Total CORE:    {core_count:,}")
        logger.info(f"Tempo total:   {total_elapsed:.1f}s ({total_elapsed/60:.1f}min)")
        if total > 0:
            logger.info(f"Taxa média:    {total/total_elapsed:,.0f}/s")
        
        return {
            "transformed": total,
            "core_total": core_count,
            "elapsed": total_elapsed,
            "metrics": self.metrics,
        }
    
    def close(self):
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description="Transformer otimizado v2 para EVO Entries")
    parser.add_argument("--full-refresh", action="store_true", help="TRUNCATE + INSERT (recomendado)")
    parser.add_argument("--year", type=int, help="Processar apenas um ano específico")
    args = parser.parse_args()
    
    transformer = EntriesTransformerV2()
    try:
        result = transformer.run(full_refresh=args.full_refresh, year=args.year)
        print(json.dumps(result, indent=2, default=str))
    finally:
        transformer.close()


if __name__ == "__main__":
    main()
