# -*- coding: utf-8 -*-
"""
Transformer: EVO Prospects STG → CORE

Normaliza prospects do JSONB para tabela tipada.

Uso:
    cd C:/skyfit-datalake/evo_prospects
    python src/transformers/normalize_evo_prospects.py
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

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
                load_dotenv(p, override=False)
                print(f"[ENV] Carregado: {p}")
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

log_file = LOG_DIR / f"normalize_prospects_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class ProspectsTransformer:
    """Transforma prospects de JSONB para tabela tipada."""
    
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
    
    def transform(self) -> int:
        """Transforma prospects."""
        logger.info("Transformando prospects STG → CORE...")
        
        sql = """
        INSERT INTO core.evo_prospects (
            prospect_id,
            branch_id,
            branch_name,
            first_name,
            last_name,
            document,
            email,
            cellphone,
            gender,
            birth_date,
            address,
            address_number,
            complement,
            neighborhood,
            city,
            state,
            country,
            zip_code,
            signup_type,
            mkt_channel,
            current_step,
            gympass_id,
            conversion_date,
            member_id,
            responsible_name,
            responsible_document,
            responsible_is_financial,
            register_date,
            custom_fields,
            _source_run_id,
            _loaded_at,
            _updated_at
        )
        SELECT 
            prospect_id,
            (raw_data->>'idBranch')::BIGINT,
            raw_data->>'branchName',
            raw_data->>'firstName',
            raw_data->>'lastName',
            raw_data->>'document',
            raw_data->>'email',
            raw_data->>'cellphone',
            raw_data->>'gender',
            (raw_data->>'birthDate')::DATE,
            raw_data->>'address',
            raw_data->>'number',
            raw_data->>'complement',
            raw_data->>'neighborhood',
            raw_data->>'city',
            raw_data->>'state',
            raw_data->>'country',
            raw_data->>'zipCode',
            raw_data->>'signupType',
            raw_data->>'mktChannel',
            raw_data->>'currentStep',
            raw_data->>'gympassId',
            (raw_data->>'conversionDate')::TIMESTAMPTZ,
            (raw_data->>'idMember')::BIGINT,
            -- Primeiro responsável (se existir)
            (raw_data->'financiallyResponsibles'->0->>'name'),
            (raw_data->'financiallyResponsibles'->0->>'cpf'),
            (raw_data->'financiallyResponsibles'->0->>'financialResponsible')::BOOLEAN,
            (raw_data->>'registerDate')::TIMESTAMPTZ,
            -- Campos customizados como JSONB
            CASE 
                WHEN raw_data->'interests' IS NOT NULL 
                THEN jsonb_build_object('interests', raw_data->'interests', 'notes', raw_data->>'notes', 'temperature', raw_data->>'temperature')
                ELSE NULL
            END,
            run_id,
            _loaded_at,
            NOW()
        FROM stg_evo.prospects_raw
        ON CONFLICT (prospect_id) DO UPDATE SET
            branch_id = EXCLUDED.branch_id,
            branch_name = EXCLUDED.branch_name,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            document = EXCLUDED.document,
            email = EXCLUDED.email,
            cellphone = EXCLUDED.cellphone,
            gender = EXCLUDED.gender,
            birth_date = EXCLUDED.birth_date,
            address = EXCLUDED.address,
            address_number = EXCLUDED.address_number,
            complement = EXCLUDED.complement,
            neighborhood = EXCLUDED.neighborhood,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            zip_code = EXCLUDED.zip_code,
            signup_type = EXCLUDED.signup_type,
            mkt_channel = EXCLUDED.mkt_channel,
            current_step = EXCLUDED.current_step,
            gympass_id = EXCLUDED.gympass_id,
            conversion_date = EXCLUDED.conversion_date,
            member_id = EXCLUDED.member_id,
            responsible_name = EXCLUDED.responsible_name,
            responsible_document = EXCLUDED.responsible_document,
            responsible_is_financial = EXCLUDED.responsible_is_financial,
            register_date = EXCLUDED.register_date,
            custom_fields = EXCLUDED.custom_fields,
            _source_run_id = EXCLUDED._source_run_id,
            _updated_at = NOW()
        """
        
        with self.conn.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
        
        self.conn.commit()
        logger.info(f"Prospects transformados: {count:,}")
        return count
    
    def get_stats(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM stg_evo.prospects_raw")
            stg = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM core.evo_prospects")
            core = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM core.evo_prospects WHERE is_converted")
            converted = cur.fetchone()[0]
        return {"stg": stg, "core": core, "converted": converted}
    
    def run(self):
        logger.info("="*60)
        logger.info("EVO PROSPECTS - TRANSFORMAÇÃO CORE")
        logger.info("="*60)
        
        start = datetime.now()
        count = self.transform()
        elapsed = (datetime.now() - start).total_seconds()
        
        stats = self.get_stats()
        
        logger.info("="*60)
        logger.info("RESULTADO")
        logger.info("="*60)
        logger.info(f"Transformados: {count:,}")
        logger.info(f"STG: {stats['stg']:,} | CORE: {stats['core']:,} | Convertidos: {stats['converted']:,}")
        logger.info(f"Tempo: {elapsed:.1f}s")
        
        return {"transformed": count, "elapsed": elapsed, "stats": stats}
    
    def close(self):
        if self.conn:
            self.conn.close()


def main():
    transformer = ProspectsTransformer()
    try:
        result = transformer.run()
        print(json.dumps(result, indent=2, default=str))
    finally:
        transformer.close()


if __name__ == "__main__":
    main()
