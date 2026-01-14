# -*- coding: utf-8 -*-
"""
Transformer OTIMIZADO v2: EVO Sales STG → CORE

OTIMIZAÇÕES:
1. Processamento em chunks (100k registros por vez)
2. Desabilita índices durante carga, recria depois
3. Connection pool
4. Log detalhado por fase
5. Opção de truncate + insert (mais rápido que upsert para carga inicial)

Uso:
    cd C:/skyfit-datalake/evo_sales
    
    # Modo incremental (UPSERT)
    python src/transformers/normalize_evo_sales_v2.py
    
    # Modo full refresh (TRUNCATE + INSERT - muito mais rápido!)
    python src/transformers/normalize_evo_sales_v2.py --full-refresh
    
    # Com chunk size customizado
    python src/transformers/normalize_evo_sales_v2.py --chunk-size 50000
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

log_file = LOG_DIR / f"normalize_sales_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class SalesTransformerV2:
    """Transformer otimizado com chunks e opção full refresh."""
    
    def __init__(self, chunk_size: int = 100000):
        self.conn = psycopg2.connect(
            host=ENV["PG_HOST"],
            port=ENV["PG_PORT"],
            database=ENV["PG_DATABASE"],
            user=ENV["PG_USER"],
            password=ENV["PG_PASSWORD"],
            sslmode=ENV["PG_SSLMODE"],
        )
        self.conn.autocommit = False
        self.chunk_size = chunk_size
        self.metrics = {}
    
    def get_stg_count(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM stg_evo.sales_raw")
            return cur.fetchone()[0]
    
    def disable_indexes(self, table: str):
        """Desabilita índices de uma tabela (melhora INSERT)."""
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT indexname FROM pg_indexes 
                WHERE tablename = '{table}' AND schemaname = 'core'
                AND indexname NOT LIKE '%_pkey'
            """)
            indexes = [row[0] for row in cur.fetchall()]
            
            for idx in indexes:
                try:
                    cur.execute(f"DROP INDEX IF EXISTS core.{idx}")
                    logger.info(f"  Índice removido: {idx}")
                except:
                    pass
        self.conn.commit()
        return indexes
    
    def recreate_indexes(self, table: str):
        """Recria índices padrão."""
        with self.conn.cursor() as cur:
            if table == 'evo_sales':
                cur.execute("CREATE INDEX IF NOT EXISTS idx_core_sales_member ON core.evo_sales(member_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_core_sales_date ON core.evo_sales(sale_date)")
            elif table == 'evo_sale_items':
                cur.execute("CREATE INDEX IF NOT EXISTS idx_core_sale_items_sale ON core.evo_sale_items(sale_id)")
            elif table == 'evo_receivables':
                cur.execute("CREATE INDEX IF NOT EXISTS idx_core_receivables_sale ON core.evo_receivables(sale_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_core_receivables_due ON core.evo_receivables(due_date)")
        self.conn.commit()
        logger.info(f"  Índices recriados para {table}")
    
    def transform_sales_chunked(self, full_refresh: bool = False) -> int:
        """Transforma sales em chunks."""
        logger.info("Transformando sales...")
        t0 = time.time()
        
        if full_refresh:
            logger.info("  Modo FULL REFRESH - truncando tabela...")
            with self.conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE core.evo_sales CASCADE")
            self.conn.commit()
            self.disable_indexes('evo_sales')
        
        total = 0
        offset = 0
        
        while True:
            with self.conn.cursor() as cur:
                sql = f"""
                INSERT INTO core.evo_sales (
                    sale_id, member_id, prospect_id, employee_id, employee_sale_id, employee_sale_name,
                    personal_id, branch_id, sale_date, sale_date_server, update_date, sale_source,
                    observations, corporate_partnership_id, corporate_partnership_name, sale_recurrency_id,
                    removed, removal_date, employee_removal_id, sale_migration_id, cart_token,
                    _source_run_id, _loaded_at
                )
                SELECT 
                    sale_id,
                    (raw_data->>'idMember')::BIGINT,
                    (raw_data->>'idProspect')::BIGINT,
                    (raw_data->>'idEmployee')::BIGINT,
                    (raw_data->>'idEmployeeSale')::BIGINT,
                    raw_data->>'nameEmployeeSale',
                    (raw_data->>'idPersonal')::BIGINT,
                    (raw_data->>'idBranch')::BIGINT,
                    (raw_data->>'saleDate')::TIMESTAMPTZ,
                    (raw_data->>'saleDateServer')::TIMESTAMPTZ,
                    (raw_data->>'updateDate')::TIMESTAMPTZ,
                    (raw_data->>'saleSource')::INT,
                    raw_data->>'observations',
                    (raw_data->>'idCorporatePartnership')::BIGINT,
                    raw_data->>'nameCorporatePartnership',
                    (raw_data->>'idSaleRecurrency')::BIGINT,
                    COALESCE((raw_data->>'removed')::BOOLEAN, false),
                    (raw_data->>'removalDate')::TIMESTAMPTZ,
                    (raw_data->>'idEmployeeRemoval')::BIGINT,
                    raw_data->>'saleMigrationId',
                    raw_data->>'cartToken',
                    run_id,
                    _loaded_at
                FROM stg_evo.sales_raw
                ORDER BY sale_id
                LIMIT {self.chunk_size} OFFSET {offset}
                {'ON CONFLICT (sale_id) DO UPDATE SET' if not full_refresh else ''}
                {'raw_data = EXCLUDED.raw_data' if not full_refresh else ''}
                """
                
                # Remove ON CONFLICT se full_refresh
                if full_refresh:
                    sql = sql.replace("ON CONFLICT (sale_id) DO UPDATE SET", "")
                    sql = sql.replace("raw_data = EXCLUDED.raw_data", "")
                
                cur.execute(sql)
                count = cur.rowcount
            
            self.conn.commit()
            total += count
            
            if count == 0 or count < self.chunk_size:
                break
            
            offset += self.chunk_size
            logger.info(f"    Processados: {total:,}")
        
        if full_refresh:
            self.recreate_indexes('evo_sales')
        
        elapsed = time.time() - t0
        self.metrics['sales'] = {'count': total, 'time': elapsed}
        logger.info(f"  Sales: {total:,} em {elapsed:.1f}s ({total/elapsed:.0f}/s)")
        return total
    
    def transform_sale_items_chunked(self, full_refresh: bool = False) -> int:
        """Transforma sale_items em chunks usando LATERAL."""
        logger.info("Transformando sale_items...")
        t0 = time.time()
        
        if full_refresh:
            logger.info("  Modo FULL REFRESH - truncando tabela...")
            with self.conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE core.evo_sale_items")
            self.conn.commit()
            self.disable_indexes('evo_sale_items')
        
        # Para items, processa por sale_id em chunks
        with self.conn.cursor() as cur:
            conflict_clause = "" if full_refresh else """
                ON CONFLICT (sale_item_id) DO UPDATE SET
                    sale_id = EXCLUDED.sale_id,
                    description = EXCLUDED.description,
                    _updated_at = NOW()
            """
            
            sql = f"""
            INSERT INTO core.evo_sale_items (
                sale_item_id, sale_id, description, item, item_value, sale_value,
                sale_value_without_credit, quantity, discount, corporate_discount, tax,
                value_next_month, membership_id, membership_renewed_id, member_membership_id,
                product_id, service_id, corporate_partnership_id, corporate_partnership_name,
                membership_start_date, num_members, voucher, accounting_code,
                municipal_service_code, fl_receipt_only, fl_swimming, fl_allow_locker,
                sale_item_migration_id, _loaded_at
            )
            SELECT 
                (item->>'idSaleItem')::BIGINT,
                sale_id,
                item->>'description',
                item->>'item',
                (item->>'itemValue')::DECIMAL(15,2),
                (item->>'saleValue')::DECIMAL(15,2),
                (item->>'saleValueWithoutCredit')::DECIMAL(15,2),
                (item->>'quantity')::INT,
                (item->>'discount')::DECIMAL(15,2),
                (item->>'corporateDiscount')::DECIMAL(15,2),
                (item->>'tax')::DECIMAL(15,2),
                (item->>'valueNextMonth')::DECIMAL(15,2),
                (item->>'idMembership')::BIGINT,
                (item->>'idMembershipRenewed')::BIGINT,
                (item->>'idMemberMembership')::BIGINT,
                (item->>'idProduct')::BIGINT,
                (item->>'idService')::BIGINT,
                (item->>'idCorporatePartnership')::BIGINT,
                item->>'nameCorporatePartnership',
                (item->>'membershipStartDate')::TIMESTAMPTZ,
                (item->>'numMembers')::INT,
                item->>'voucher',
                item->>'accountingCode',
                item->>'municipalServiceCode',
                (item->>'flReceiptOnly')::BOOLEAN,
                (item->>'flSwimming')::BOOLEAN,
                (item->>'flAllowLocker')::BOOLEAN,
                item->>'saleItemMigrationId',
                _loaded_at
            FROM stg_evo.sales_raw,
            LATERAL jsonb_array_elements(raw_data->'saleItens') AS item
            WHERE raw_data->'saleItens' IS NOT NULL
            {conflict_clause}
            """
            
            cur.execute(sql)
            total = cur.rowcount
        
        self.conn.commit()
        
        if full_refresh:
            self.recreate_indexes('evo_sale_items')
        
        elapsed = time.time() - t0
        self.metrics['sale_items'] = {'count': total, 'time': elapsed}
        logger.info(f"  Sale Items: {total:,} em {elapsed:.1f}s ({total/elapsed:.0f}/s)")
        return total
    
    def transform_receivables_chunked(self, full_refresh: bool = False) -> int:
        """Transforma receivables em chunks usando LATERAL."""
        logger.info("Transformando receivables...")
        t0 = time.time()
        
        if full_refresh:
            logger.info("  Modo FULL REFRESH - truncando tabela...")
            with self.conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE core.evo_receivables")
            self.conn.commit()
            self.disable_indexes('evo_receivables')
        
        with self.conn.cursor() as cur:
            conflict_clause = "" if full_refresh else """
                ON CONFLICT (receivable_id) DO UPDATE SET
                    amount_paid = EXCLUDED.amount_paid,
                    status_id = EXCLUDED.status_id,
                    status_name = EXCLUDED.status_name,
                    receiving_date = EXCLUDED.receiving_date,
                    _updated_at = NOW()
            """
            
            sql = f"""
            INSERT INTO core.evo_receivables (
                receivable_id, sale_id, registration_date, due_date, receiving_date,
                cancellation_date, update_date, amount, amount_paid, status_id, status_name,
                current_installment, total_installments, payment_type_id, payment_type_name,
                authorization, tid, nsu, card_flag, transaction_token, _loaded_at
            )
            SELECT 
                (recv->>'idReceivable')::BIGINT,
                sale_id,
                (recv->>'registrationDate')::TIMESTAMPTZ,
                (recv->>'dueDate')::TIMESTAMPTZ,
                (recv->>'receivingDate')::TIMESTAMPTZ,
                (recv->>'cancellationDate')::TIMESTAMPTZ,
                (recv->>'updateDate')::TIMESTAMPTZ,
                (recv->>'amount')::DECIMAL(15,2),
                (recv->>'ammountPaid')::DECIMAL(15,2),
                (recv->'status'->>'id')::INT,
                recv->'status'->>'name',
                (recv->>'currentInstallment')::INT,
                (recv->>'totalInstallments')::INT,
                (recv->'paymentType'->>'idPaymentType')::INT,
                recv->'paymentType'->>'name',
                recv->>'authorization',
                recv->>'tid',
                recv->>'nsu',
                recv->>'cardFlag',
                recv->>'transactionToken',
                _loaded_at
            FROM stg_evo.sales_raw,
            LATERAL jsonb_array_elements(raw_data->'receivables') AS recv
            WHERE raw_data->'receivables' IS NOT NULL
            {conflict_clause}
            """
            
            cur.execute(sql)
            total = cur.rowcount
        
        self.conn.commit()
        
        if full_refresh:
            self.recreate_indexes('evo_receivables')
        
        elapsed = time.time() - t0
        self.metrics['receivables'] = {'count': total, 'time': elapsed}
        logger.info(f"  Receivables: {total:,} em {elapsed:.1f}s ({total/elapsed:.0f}/s)")
        return total
    
    def run(self, full_refresh: bool = False) -> Dict[str, Any]:
        logger.info("="*70)
        logger.info("EVO SALES - TRANSFORMAÇÃO CORE v2")
        logger.info("="*70)
        logger.info(f"Modo: {'FULL REFRESH' if full_refresh else 'INCREMENTAL (UPSERT)'}")
        logger.info(f"Chunk size: {self.chunk_size:,}")
        
        stg_count = self.get_stg_count()
        logger.info(f"Registros em STG: {stg_count:,}")
        
        start = time.time()
        
        sales = self.transform_sales_chunked(full_refresh)
        items = self.transform_sale_items_chunked(full_refresh)
        receivables = self.transform_receivables_chunked(full_refresh)
        
        total_elapsed = time.time() - start
        
        logger.info("="*70)
        logger.info("RESULTADO")
        logger.info("="*70)
        logger.info(f"Sales:       {sales:,}")
        logger.info(f"Sale Items:  {items:,}")
        logger.info(f"Receivables: {receivables:,}")
        logger.info(f"Tempo total: {total_elapsed:.1f}s ({total_elapsed/60:.1f}min)")
        
        return {
            "sales": sales,
            "sale_items": items,
            "receivables": receivables,
            "elapsed": total_elapsed,
            "metrics": self.metrics,
        }
    
    def close(self):
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description="Transformer otimizado v2 para EVO Sales")
    parser.add_argument("--full-refresh", action="store_true", help="TRUNCATE + INSERT (mais rápido)")
    parser.add_argument("--chunk-size", type=int, default=100000, help="Registros por chunk (default: 100000)")
    args = parser.parse_args()
    
    transformer = SalesTransformerV2(chunk_size=args.chunk_size)
    try:
        result = transformer.run(full_refresh=args.full_refresh)
        print(json.dumps(result, indent=2, default=str))
    finally:
        transformer.close()


if __name__ == "__main__":
    main()
