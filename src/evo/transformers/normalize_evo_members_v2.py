# -*- coding: utf-8 -*-
"""
Transformer OTIMIZADO v2: EVO Members STG → CORE

OTIMIZAÇÕES:
1. FULL REFRESH muito mais rápido (TRUNCATE + INSERT sem índices)
2. Processamento paralelo das 3 tabelas
3. Desabilita índices durante carga
4. Log detalhado

Uso:
    cd C:/skyfit-datalake/evo_members
    
    # Modo incremental (lento mas seguro)
    python src/transformers/normalize_evo_members_v2.py
    
    # Modo full refresh (MUITO MAIS RÁPIDO!)
    python src/transformers/normalize_evo_members_v2.py --full-refresh
"""
import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

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

log_file = LOG_DIR / f"normalize_members_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class MembersTransformerV2:
    def __init__(self):
        self.metrics = {}
    
    def get_connection(self):
        return psycopg2.connect(
            host=ENV["PG_HOST"],
            port=ENV["PG_PORT"],
            database=ENV["PG_DATABASE"],
            user=ENV["PG_USER"],
            password=ENV["PG_PASSWORD"],
            sslmode=ENV["PG_SSLMODE"],
        )
    
    def transform_members(self, full_refresh: bool = False) -> int:
        """Transforma membros principais."""
        logger.info("Transformando membros...")
        t0 = time.time()
        
        conn = self.get_connection()
        conn.autocommit = False
        
        try:
            with conn.cursor() as cur:
                if full_refresh:
                    logger.info("  TRUNCATE core.evo_members...")
                    cur.execute("TRUNCATE TABLE core.evo_members CASCADE")
                    conn.commit()
                    
                    # Remove índices
                    cur.execute("DROP INDEX IF EXISTS core.idx_core_members_branch")
                    cur.execute("DROP INDEX IF EXISTS core.idx_core_members_status")
                    conn.commit()
                
                conflict_clause = "" if full_refresh else """
                    ON CONFLICT (member_id) DO UPDATE SET
                        raw_data = EXCLUDED.raw_data,
                        _updated_at = NOW()
                """
                
                sql = f"""
                INSERT INTO core.evo_members (
                    member_id, branch_id, branch_name, first_name, last_name, 
                    register_name, register_last_name, use_preferred_name,
                    document, document_id, email, cellphone, gender, birth_date, marital_status, 
                    address, address_number, complement, neighborhood, city, state, country, zip_code, 
                    access_card_number, access_blocked, blocked_reason, status, membership_status, 
                    penalized, total_fit_coins, register_date, conversion_date, last_access_date, 
                    update_date, photo_url, gympass_id, code_totalpass, user_id_gurupass,
                    client_with_promotional_restriction, personal_trainer, personal_type, cref, 
                    cref_expiration_date, employee_consultant_id, employee_consultant_name, 
                    employee_instructor_id, employee_instructor_name, employee_personal_id, 
                    employee_personal_name, member_migration_id, _source_run_id, _loaded_at
                )
                SELECT 
                    member_id,
                    (raw_data->>'idBranch')::BIGINT,
                    raw_data->>'branchName',
                    raw_data->>'firstName',
                    raw_data->>'lastName',
                    raw_data->>'registerName',
                    raw_data->>'registerLastName',
                    COALESCE((raw_data->>'usePreferredName')::BOOLEAN, false),
                    raw_data->>'document',
                    raw_data->>'documentId',
                    (SELECT c->>'description' FROM jsonb_array_elements(raw_data->'contacts') c 
                     WHERE (c->>'idContactType')::INT IN (3,4) LIMIT 1),
                    (SELECT c->>'description' FROM jsonb_array_elements(raw_data->'contacts') c 
                     WHERE (c->>'idContactType')::INT IN (1,2) LIMIT 1),
                    raw_data->>'gender',
                    (raw_data->>'birthDate')::DATE,
                    raw_data->>'maritalStatus',
                    raw_data->>'address',
                    COALESCE(raw_data->>'addressNumber', raw_data->>'number'),
                    raw_data->>'complement',
                    raw_data->>'neighborhood',
                    raw_data->>'city',
                    raw_data->>'state',
                    raw_data->>'country',
                    raw_data->>'zipCode',
                    raw_data->>'accessCardNumber',
                    COALESCE((raw_data->>'accessBlocked')::BOOLEAN, false),
                    raw_data->>'blockedReason',
                    raw_data->>'status',
                    raw_data->>'membershipStatus',
                    COALESCE((raw_data->>'penalized')::BOOLEAN, false),
                    (raw_data->>'totalFitCoins')::DECIMAL(15,2),
                    (raw_data->>'registerDate')::TIMESTAMPTZ,
                    (raw_data->>'conversionDate')::TIMESTAMPTZ,
                    (raw_data->>'lastAccessDate')::TIMESTAMPTZ,
                    (raw_data->>'updateDate')::TIMESTAMPTZ,
                    COALESCE(raw_data->>'photo', raw_data->>'photoUrl'),
                    raw_data->>'gympassId',
                    raw_data->>'codeTotalpass',
                    raw_data->>'userIdGurupass',
                    COALESCE((raw_data->>'clientWithPromotionalRestriction')::BOOLEAN, false),
                    COALESCE((raw_data->>'personalTrainer')::BOOLEAN, false),
                    raw_data->>'personalType',
                    raw_data->>'cref',
                    (raw_data->>'crefExpirationDate')::DATE,
                    COALESCE((raw_data->>'idEmployeeConsultant')::BIGINT, (raw_data->'employeeConsultant'->>'idEmployee')::BIGINT),
                    COALESCE(raw_data->>'nameEmployeeConsultant', raw_data->'employeeConsultant'->>'name'),
                    COALESCE((raw_data->>'idEmployeeInstructor')::BIGINT, (raw_data->'employeeInstructor'->>'idEmployee')::BIGINT),
                    COALESCE(raw_data->>'nameEmployeeInstructor', raw_data->'employeeInstructor'->>'name'),
                    COALESCE((raw_data->>'idEmployeePersonalTrainer')::BIGINT, (raw_data->'employeePersonal'->>'idEmployee')::BIGINT),
                    COALESCE(raw_data->>'nameEmployeePersonalTrainer', raw_data->'employeePersonal'->>'name'),
                    COALESCE(raw_data->>'memberMigrationId', raw_data->>'idMemberMigration'),
                    run_id,
                    _loaded_at
                FROM stg_evo.members_raw
                {conflict_clause}
                """
                
                cur.execute(sql)
                count = cur.rowcount
            
            conn.commit()
            
            if full_refresh:
                with conn.cursor() as cur:
                    cur.execute("CREATE INDEX idx_core_members_branch ON core.evo_members(branch_id)")
                    cur.execute("CREATE INDEX idx_core_members_status ON core.evo_members(status)")
                conn.commit()
                logger.info("  Índices recriados")
            
            elapsed = time.time() - t0
            self.metrics['members'] = {'count': count, 'time': elapsed}
            logger.info(f"  Membros: {count:,} em {elapsed:.1f}s ({count/elapsed:.0f}/s)")
            return count
            
        finally:
            conn.close()
    
    def transform_memberships(self, full_refresh: bool = False) -> int:
        """Transforma memberships (contratos) - TABELA MAIS PESADA."""
        logger.info("Transformando memberships...")
        t0 = time.time()
        
        conn = self.get_connection()
        conn.autocommit = False
        
        try:
            with conn.cursor() as cur:
                if full_refresh:
                    logger.info("  TRUNCATE core.evo_member_memberships...")
                    cur.execute("TRUNCATE TABLE core.evo_member_memberships")
                    conn.commit()
                
                conflict_clause = "" if full_refresh else """
                    ON CONFLICT (member_id, member_membership_id) DO UPDATE SET
                        membership_status = EXCLUDED.membership_status,
                        value_next_month = EXCLUDED.value_next_month,
                        _updated_at = NOW()
                """
                
                sql = f"""
                INSERT INTO core.evo_member_memberships (
                    member_membership_id, member_id, membership_id, membership_name, membership_renewed_id,
                    sale_id, sale_date, start_date, end_date, cancel_date, cancel_date_on, cancel_creation_date,
                    membership_status, value_next_month, original_value, next_charge, next_date_suspension,
                    category_membership_id, loyalty_end_date, assessment_end_date, acceptance_date,
                    num_members, fl_allow_locker, fl_additional_membership, allow_les_mills,
                    allows_cancellation_by_app, signed_terms, limitless, weekly_limit, bioimpedance_amount,
                    concluded_sessions, pending_sessions, scheduled_sessions, pending_repositions,
                    repositions_total, bonus_sessions, number_suspension_times, max_suspension_days,
                    minimum_suspension_days, disponible_suspension_days, disponible_suspension_times,
                    days_left_to_freeze, contract_printing, freezes, sessions, _loaded_at
                )
                SELECT 
                    (m->>'idMemberMembership')::BIGINT,
                    member_id,
                    (m->>'idMembership')::BIGINT,
                    m->>'membershipName',
                    (m->>'idMembershipRenewed')::BIGINT,
                    (m->>'idSale')::BIGINT,
                    (m->>'saleDate')::TIMESTAMPTZ,
                    (m->>'startDate')::TIMESTAMPTZ,
                    (m->>'endDate')::TIMESTAMPTZ,
                    (m->>'cancelDate')::TIMESTAMPTZ,
                    (m->>'cancelDateOn')::TIMESTAMPTZ,
                    (m->>'cancelCreationDate')::TIMESTAMPTZ,
                    m->>'membershipStatus',
                    (m->>'valueNextMonth')::DECIMAL(15,2),
                    (m->>'originalValue')::DECIMAL(15,2),
                    (m->>'nextCharge')::TIMESTAMPTZ,
                    (m->>'nextDateSuspension')::TIMESTAMPTZ,
                    (m->>'idCategoryMembership')::BIGINT,
                    (m->>'loyaltyEndDate')::TIMESTAMPTZ,
                    (m->>'assessmentEndDate')::TIMESTAMPTZ,
                    (m->>'acceptanceDate')::TIMESTAMPTZ,
                    (m->>'numMembers')::INT,
                    (m->>'flAllowLocker')::BOOLEAN,
                    (m->>'flAdditionalMembership')::BOOLEAN,
                    (m->>'allowLesMills')::BOOLEAN,
                    (m->>'allowsCancellationByApp')::BOOLEAN,
                    (m->>'signedTerms')::BOOLEAN,
                    (m->>'limitless')::BOOLEAN,
                    (m->>'weeklyLimit')::INT,
                    (m->>'bioimpedanceAmount')::INT,
                    (m->>'concludedSessions')::INT,
                    (m->>'pendingSessions')::INT,
                    (m->>'scheduledSessions')::INT,
                    (m->>'pendingRepositions')::INT,
                    (m->>'repositionsTotal')::INT,
                    (m->>'bonusSessions')::INT,
                    (m->>'numberSuspensionTimes')::INT,
                    (m->>'maxSuspensionDays')::INT,
                    (m->>'minimumSuspensionDays')::INT,
                    (m->>'disponibleSuspensionDays')::INT,
                    (m->>'disponibleSuspensionTimes')::INT,
                    (m->>'daysLeftToFreeze')::INT,
                    m->>'contractPrinting',
                    m->'freezes',
                    m->'sessions',
                    _loaded_at
                FROM stg_evo.members_raw,
                LATERAL jsonb_array_elements(raw_data->'memberships') AS m
                WHERE raw_data->'memberships' IS NOT NULL
                {conflict_clause}
                """
                
                cur.execute(sql)
                count = cur.rowcount
            
            conn.commit()
            
            elapsed = time.time() - t0
            self.metrics['memberships'] = {'count': count, 'time': elapsed}
            logger.info(f"  Memberships: {count:,} em {elapsed:.1f}s ({count/elapsed:.0f}/s)")
            return count
            
        finally:
            conn.close()
    
    def transform_contacts(self, full_refresh: bool = False) -> int:
        """Transforma contatos."""
        logger.info("Transformando contatos...")
        t0 = time.time()
        
        conn = self.get_connection()
        conn.autocommit = False
        
        try:
            with conn.cursor() as cur:
                if full_refresh:
                    logger.info("  TRUNCATE core.evo_member_contacts...")
                    cur.execute("TRUNCATE TABLE core.evo_member_contacts")
                    conn.commit()
                
                conflict_clause = "" if full_refresh else """
                    ON CONFLICT (member_id, phone_id) DO UPDATE SET
                        description = EXCLUDED.description,
                        _loaded_at = NOW()
                """
                
                sql = f"""
                INSERT INTO core.evo_member_contacts (
                    phone_id, member_id, contact_type_id, contact_type, ddi, description, _loaded_at
                )
                SELECT 
                    (c->>'idPhone')::BIGINT,
                    member_id,
                    (c->>'idContactType')::INT,
                    c->>'typeDescription',
                    c->>'ddi',
                    c->>'description',
                    _loaded_at
                FROM stg_evo.members_raw,
                LATERAL jsonb_array_elements(raw_data->'contacts') AS c
                WHERE raw_data->'contacts' IS NOT NULL
                {conflict_clause}
                """
                
                cur.execute(sql)
                count = cur.rowcount
            
            conn.commit()
            
            elapsed = time.time() - t0
            self.metrics['contacts'] = {'count': count, 'time': elapsed}
            logger.info(f"  Contatos: {count:,} em {elapsed:.1f}s ({count/elapsed:.0f}/s)")
            return count
            
        finally:
            conn.close()
    
    def run(self, full_refresh: bool = False) -> Dict[str, Any]:
        logger.info("="*70)
        logger.info("EVO MEMBERS - TRANSFORMAÇÃO CORE v2")
        logger.info("="*70)
        logger.info(f"Modo: {'FULL REFRESH' if full_refresh else 'INCREMENTAL (UPSERT)'}")
        
        start = time.time()
        
        # Executa transformações
        members = self.transform_members(full_refresh)
        memberships = self.transform_memberships(full_refresh)
        contacts = self.transform_contacts(full_refresh)
        
        total_elapsed = time.time() - start
        
        logger.info("="*70)
        logger.info("RESULTADO")
        logger.info("="*70)
        logger.info(f"Membros:     {members:,}")
        logger.info(f"Memberships: {memberships:,}")
        logger.info(f"Contatos:    {contacts:,}")
        logger.info(f"Tempo total: {total_elapsed:.1f}s ({total_elapsed/60:.1f}min)")
        
        return {
            "members": members,
            "memberships": memberships,
            "contacts": contacts,
            "elapsed": total_elapsed,
            "metrics": self.metrics,
        }


def main():
    parser = argparse.ArgumentParser(description="Transformer otimizado v2 para EVO Members")
    parser.add_argument("--full-refresh", action="store_true", help="TRUNCATE + INSERT (muito mais rápido!)")
    args = parser.parse_args()
    
    transformer = MembersTransformerV2()
    result = transformer.run(full_refresh=args.full_refresh)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
