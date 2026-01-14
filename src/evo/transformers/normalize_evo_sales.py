# -*- coding: utf-8 -*-
"""
Transformer: EVO Sales STG → CORE

Normaliza vendas do JSONB para tabelas tipadas:
- core.evo_sales (tabela principal)
- core.evo_sale_items (itens da venda - 1:N)
- core.evo_receivables (recebíveis/títulos - 1:N)

Uso:
    cd C:/skyfit-datalake/evo_sales
    python src/transformers/normalize_evo_sales.py
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

log_file = LOG_DIR / f"normalize_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class SalesTransformer:
    """Transforma vendas de JSONB para tabelas normalizadas."""
    
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
    
    def transform_sales(self) -> int:
        """Transforma tabela principal de vendas."""
        logger.info("Transformando vendas principais...")
        
        sql = """
        INSERT INTO core.evo_sales (
            sale_id, member_id, prospect_id, employee_id, employee_sale_id,
            employee_sale_name, personal_id, branch_id, sale_date, sale_date_server,
            update_date, sale_source, observations, corporate_partnership_id,
            corporate_partnership_name, sale_recurrency_id, removed, removal_date,
            employee_removal_id, sale_migration_id, cart_token, _source_run_id,
            _loaded_at, _updated_at
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
            (raw_data->>'coporatePartnershipId')::BIGINT,
            raw_data->>'corporatePartnershipName',
            (raw_data->>'idSaleRecurrency')::BIGINT,
            COALESCE((raw_data->>'removed')::BOOLEAN, FALSE),
            (raw_data->>'removalDate')::TIMESTAMPTZ,
            (raw_data->>'idEmployeeRemoval')::BIGINT,
            raw_data->>'idSaleMigration',
            raw_data->>'idCartToken',
            run_id,
            _loaded_at,
            NOW()
        FROM stg_evo.sales_raw
        ON CONFLICT (sale_id) DO UPDATE SET
            member_id = EXCLUDED.member_id,
            prospect_id = EXCLUDED.prospect_id,
            employee_id = EXCLUDED.employee_id,
            employee_sale_id = EXCLUDED.employee_sale_id,
            employee_sale_name = EXCLUDED.employee_sale_name,
            personal_id = EXCLUDED.personal_id,
            branch_id = EXCLUDED.branch_id,
            sale_date = EXCLUDED.sale_date,
            sale_date_server = EXCLUDED.sale_date_server,
            update_date = EXCLUDED.update_date,
            sale_source = EXCLUDED.sale_source,
            observations = EXCLUDED.observations,
            corporate_partnership_id = EXCLUDED.corporate_partnership_id,
            corporate_partnership_name = EXCLUDED.corporate_partnership_name,
            sale_recurrency_id = EXCLUDED.sale_recurrency_id,
            removed = EXCLUDED.removed,
            removal_date = EXCLUDED.removal_date,
            employee_removal_id = EXCLUDED.employee_removal_id,
            sale_migration_id = EXCLUDED.sale_migration_id,
            cart_token = EXCLUDED.cart_token,
            _source_run_id = EXCLUDED._source_run_id,
            _updated_at = NOW()
        """
        
        with self.conn.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
        
        self.conn.commit()
        logger.info(f"  Vendas: {count:,}")
        return count
    
    def transform_sale_items(self) -> int:
        """Transforma itens de venda (explode array saleItens)."""
        logger.info("Transformando itens de venda...")
        
        sql = """
        INSERT INTO core.evo_sale_items (
            sale_item_id, sale_id, description, item, item_value, sale_value,
            sale_value_without_credit, quantity, discount, corporate_discount,
            tax, value_next_month, membership_id, membership_renewed_id,
            member_membership_id, product_id, service_id, corporate_partnership_id,
            corporate_partnership_name, membership_start_date, num_members,
            voucher, accounting_code, municipal_service_code, fl_receipt_only,
            fl_swimming, fl_allow_locker, sale_item_migration_id, _loaded_at, _updated_at
        )
        SELECT 
            (item->>'idSaleItem')::BIGINT,
            sale_id,
            item->>'description',
            item->>'item',
            (item->>'itemValue')::DECIMAL(15,2),
            (item->>'saleValue')::DECIMAL(15,2),
            (item->>'saleValueWithoutCreditValue')::DECIMAL(15,2),
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
            (item->>'coporatePartnershipId')::BIGINT,
            item->>'corporatePartnershipName',
            (item->>'membershipStartDate')::TIMESTAMPTZ,
            (item->>'numMembers')::INT,
            item->>'voucher',
            item->>'accountingCode',
            item->>'municipalServiceCode',
            (item->>'flReceiptOnly')::BOOLEAN,
            (item->>'flSwimming')::BOOLEAN,
            (item->>'flAllowLocker')::BOOLEAN,
            item->>'idSaleItemMigration',
            _loaded_at,
            NOW()
        FROM stg_evo.sales_raw,
        LATERAL jsonb_array_elements(
            CASE 
                WHEN raw_data->'saleItens' IS NOT NULL 
                     AND jsonb_typeof(raw_data->'saleItens') = 'array'
                THEN raw_data->'saleItens'
                ELSE '[]'::jsonb
            END
        ) AS item
        WHERE (item->>'idSaleItem') IS NOT NULL
        ON CONFLICT (sale_item_id) DO UPDATE SET
            sale_id = EXCLUDED.sale_id,
            description = EXCLUDED.description,
            item = EXCLUDED.item,
            item_value = EXCLUDED.item_value,
            sale_value = EXCLUDED.sale_value,
            sale_value_without_credit = EXCLUDED.sale_value_without_credit,
            quantity = EXCLUDED.quantity,
            discount = EXCLUDED.discount,
            corporate_discount = EXCLUDED.corporate_discount,
            tax = EXCLUDED.tax,
            value_next_month = EXCLUDED.value_next_month,
            membership_id = EXCLUDED.membership_id,
            membership_renewed_id = EXCLUDED.membership_renewed_id,
            member_membership_id = EXCLUDED.member_membership_id,
            product_id = EXCLUDED.product_id,
            service_id = EXCLUDED.service_id,
            corporate_partnership_id = EXCLUDED.corporate_partnership_id,
            corporate_partnership_name = EXCLUDED.corporate_partnership_name,
            membership_start_date = EXCLUDED.membership_start_date,
            num_members = EXCLUDED.num_members,
            voucher = EXCLUDED.voucher,
            accounting_code = EXCLUDED.accounting_code,
            municipal_service_code = EXCLUDED.municipal_service_code,
            fl_receipt_only = EXCLUDED.fl_receipt_only,
            fl_swimming = EXCLUDED.fl_swimming,
            fl_allow_locker = EXCLUDED.fl_allow_locker,
            sale_item_migration_id = EXCLUDED.sale_item_migration_id,
            _updated_at = NOW()
        """
        
        with self.conn.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
        
        self.conn.commit()
        logger.info(f"  Itens: {count:,}")
        return count
    
    def transform_receivables(self) -> int:
        """Transforma recebíveis (explode array receivables)."""
        logger.info("Transformando recebíveis...")
        
        sql = """
        INSERT INTO core.evo_receivables (
            receivable_id, sale_id, registration_date, due_date, receiving_date,
            cancellation_date, update_date, amount, amount_paid, status_id,
            status_name, current_installment, total_installments, payment_type_id,
            payment_type_name, authorization, tid, nsu, card_flag, transaction_token,
            _loaded_at, _updated_at
        )
        SELECT 
            (rec->>'idReceivable')::BIGINT,
            sale_id,
            (rec->>'registrationDate')::TIMESTAMPTZ,
            (rec->>'dueDate')::TIMESTAMPTZ,
            (rec->>'receivingDate')::TIMESTAMPTZ,
            (rec->>'cancellationDate')::TIMESTAMPTZ,
            (rec->>'updateDate')::TIMESTAMPTZ,
            (rec->>'ammount')::DECIMAL(15,2),
            (rec->>'ammountPaid')::DECIMAL(15,2),
            (rec->'status'->>'id')::INT,
            rec->'status'->>'name',
            (rec->>'currentInstallment')::INT,
            (rec->>'totalInstallments')::INT,
            (rec->'paymentType'->>'id')::INT,
            rec->'paymentType'->>'name',
            rec->>'authorization',
            rec->>'tid',
            rec->>'nsu',
            rec->>'cardFlag',
            rec->>'transactionToken',
            _loaded_at,
            NOW()
        FROM stg_evo.sales_raw,
        LATERAL jsonb_array_elements(
            CASE 
                WHEN raw_data->'receivables' IS NOT NULL 
                     AND jsonb_typeof(raw_data->'receivables') = 'array'
                THEN raw_data->'receivables'
                ELSE '[]'::jsonb
            END
        ) AS rec
        WHERE (rec->>'idReceivable') IS NOT NULL
        ON CONFLICT (receivable_id) DO UPDATE SET
            sale_id = EXCLUDED.sale_id,
            registration_date = EXCLUDED.registration_date,
            due_date = EXCLUDED.due_date,
            receiving_date = EXCLUDED.receiving_date,
            cancellation_date = EXCLUDED.cancellation_date,
            update_date = EXCLUDED.update_date,
            amount = EXCLUDED.amount,
            amount_paid = EXCLUDED.amount_paid,
            status_id = EXCLUDED.status_id,
            status_name = EXCLUDED.status_name,
            current_installment = EXCLUDED.current_installment,
            total_installments = EXCLUDED.total_installments,
            payment_type_id = EXCLUDED.payment_type_id,
            payment_type_name = EXCLUDED.payment_type_name,
            authorization = EXCLUDED.authorization,
            tid = EXCLUDED.tid,
            nsu = EXCLUDED.nsu,
            card_flag = EXCLUDED.card_flag,
            transaction_token = EXCLUDED.transaction_token,
            _updated_at = NOW()
        """
        
        with self.conn.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
        
        self.conn.commit()
        logger.info(f"  Recebíveis: {count:,}")
        return count
    
    def get_stats(self):
        stats = {}
        tables = [
            ("stg_evo.sales_raw", "stg"),
            ("core.evo_sales", "sales"),
            ("core.evo_sale_items", "items"),
            ("core.evo_receivables", "receivables"),
        ]
        with self.conn.cursor() as cur:
            for table, key in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    stats[key] = cur.fetchone()[0]
                except:
                    stats[key] = 0
        return stats
    
    def run(self):
        logger.info("="*60)
        logger.info("EVO SALES - TRANSFORMAÇÃO CORE")
        logger.info("="*60)
        
        start = datetime.now()
        
        sales = self.transform_sales()
        items = self.transform_sale_items()
        receivables = self.transform_receivables()
        
        elapsed = (datetime.now() - start).total_seconds()
        stats = self.get_stats()
        
        logger.info("="*60)
        logger.info("RESULTADO")
        logger.info("="*60)
        logger.info(f"Vendas: {sales:,}")
        logger.info(f"Itens: {items:,}")
        logger.info(f"Recebíveis: {receivables:,}")
        logger.info(f"Tempo: {elapsed:.1f}s")
        
        return {
            "sales": sales, "items": items, "receivables": receivables,
            "elapsed": elapsed, "stats": stats
        }
    
    def close(self):
        if self.conn:
            self.conn.close()


def main():
    transformer = SalesTransformer()
    try:
        result = transformer.run()
        print(json.dumps(result, indent=2, default=str))
    finally:
        transformer.close()


if __name__ == "__main__":
    main()
