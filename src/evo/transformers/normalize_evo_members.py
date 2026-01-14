# -*- coding: utf-8 -*-
"""
Transformer: EVO Members STG → CORE

Normaliza membros do JSONB para tabelas tipadas:
- core.evo_members (tabela principal)
- core.evo_member_memberships (contratos/planos - 1:N)
- core.evo_member_contacts (telefones - 1:N)

IMPORTANTE: memberships são fundamentais para análise de churn e MRR

Uso:
    cd C:/skyfit-datalake/evo_members
    python src/transformers/normalize_evo_members.py
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

log_file = LOG_DIR / f"normalize_members_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class MembersTransformer:
    """Transforma membros de JSONB para tabelas normalizadas."""
    
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
    
    def transform_members(self) -> int:
        """Transforma tabela principal de membros."""
        logger.info("Transformando membros...")
        
        sql = """
        INSERT INTO core.evo_members (
            member_id, branch_id, branch_name, first_name, last_name,
            document, document_id, email, cellphone, gender, birth_date,
            marital_status, address, address_number, complement, neighborhood,
            city, state, country, zip_code, access_card_number, access_blocked,
            blocked_reason, status, membership_status, penalized, total_fit_coins,
            register_date, conversion_date, last_access_date, update_date,
            photo_url, gympass_id, personal_trainer, personal_type, cref,
            cref_expiration_date, employee_consultant_id, employee_consultant_name,
            employee_instructor_id, employee_instructor_name, employee_personal_id,
            employee_personal_name, member_migration_id, _source_run_id, _loaded_at, _updated_at
        )
        SELECT 
            member_id,
            (raw_data->>'idBranch')::BIGINT,
            raw_data->>'branchName',
            raw_data->>'firstName',
            raw_data->>'lastName',
            raw_data->>'document',
            raw_data->>'documentId',
            -- Email do primeiro contato tipo email
            (SELECT c->>'description' 
             FROM jsonb_array_elements(COALESCE(raw_data->'contacts', '[]'::jsonb)) c 
             WHERE (c->>'idContactType')::INT IN (3, 4) LIMIT 1),
            -- Celular do primeiro contato tipo telefone
            (SELECT c->>'description' 
             FROM jsonb_array_elements(COALESCE(raw_data->'contacts', '[]'::jsonb)) c 
             WHERE (c->>'idContactType')::INT IN (1, 2) LIMIT 1),
            raw_data->>'gender',
            (raw_data->>'birthDate')::DATE,
            raw_data->>'maritalStatus',
            raw_data->>'address',
            raw_data->>'number',
            raw_data->>'complement',
            raw_data->>'neighborhood',
            raw_data->>'city',
            raw_data->>'state',
            raw_data->>'country',
            raw_data->>'zipCode',
            raw_data->>'accessCardNumber',
            COALESCE((raw_data->>'accessBlocked')::BOOLEAN, FALSE),
            raw_data->>'blockedReason',
            raw_data->>'status',
            raw_data->>'membershipStatus',
            COALESCE((raw_data->>'penalized')::BOOLEAN, FALSE),
            (raw_data->>'totalFitCoins')::DECIMAL(15,2),
            (raw_data->>'registerDate')::TIMESTAMPTZ,
            (raw_data->>'conversionDate')::TIMESTAMPTZ,
            (raw_data->>'lastAccessDate')::TIMESTAMPTZ,
            (raw_data->>'updateDate')::TIMESTAMPTZ,
            raw_data->>'photoUrl',
            raw_data->>'gympassId',
            COALESCE((raw_data->>'personalTrainer')::BOOLEAN, FALSE),
            raw_data->>'personalType',
            raw_data->>'cref',
            (raw_data->>'crefExpirationDate')::DATE,
            (raw_data->>'idEmployeeConsultant')::BIGINT,
            raw_data->>'nameEmployeeConsultant',
            (raw_data->>'idEmployeeInstructor')::BIGINT,
            raw_data->>'nameEmployeeInstructor',
            (raw_data->>'idEmployeePersonalTrainer')::BIGINT,
            raw_data->>'nameEmployeePersonalTrainer',
            raw_data->>'idMemberMigration',
            run_id,
            _loaded_at,
            NOW()
        FROM stg_evo.members_raw
        ON CONFLICT (member_id) DO UPDATE SET
            branch_id = EXCLUDED.branch_id,
            branch_name = EXCLUDED.branch_name,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            document = EXCLUDED.document,
            document_id = EXCLUDED.document_id,
            email = EXCLUDED.email,
            cellphone = EXCLUDED.cellphone,
            gender = EXCLUDED.gender,
            birth_date = EXCLUDED.birth_date,
            marital_status = EXCLUDED.marital_status,
            address = EXCLUDED.address,
            address_number = EXCLUDED.address_number,
            complement = EXCLUDED.complement,
            neighborhood = EXCLUDED.neighborhood,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            zip_code = EXCLUDED.zip_code,
            access_card_number = EXCLUDED.access_card_number,
            access_blocked = EXCLUDED.access_blocked,
            blocked_reason = EXCLUDED.blocked_reason,
            status = EXCLUDED.status,
            membership_status = EXCLUDED.membership_status,
            penalized = EXCLUDED.penalized,
            total_fit_coins = EXCLUDED.total_fit_coins,
            register_date = EXCLUDED.register_date,
            conversion_date = EXCLUDED.conversion_date,
            last_access_date = EXCLUDED.last_access_date,
            update_date = EXCLUDED.update_date,
            photo_url = EXCLUDED.photo_url,
            gympass_id = EXCLUDED.gympass_id,
            personal_trainer = EXCLUDED.personal_trainer,
            personal_type = EXCLUDED.personal_type,
            cref = EXCLUDED.cref,
            cref_expiration_date = EXCLUDED.cref_expiration_date,
            employee_consultant_id = EXCLUDED.employee_consultant_id,
            employee_consultant_name = EXCLUDED.employee_consultant_name,
            employee_instructor_id = EXCLUDED.employee_instructor_id,
            employee_instructor_name = EXCLUDED.employee_instructor_name,
            employee_personal_id = EXCLUDED.employee_personal_id,
            employee_personal_name = EXCLUDED.employee_personal_name,
            member_migration_id = EXCLUDED.member_migration_id,
            _source_run_id = EXCLUDED._source_run_id,
            _updated_at = NOW()
        """
        
        with self.conn.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
        
        self.conn.commit()
        logger.info(f"  Membros: {count:,}")
        return count
    
    def transform_memberships(self) -> int:
        """Transforma memberships (contratos/planos) - 1:N com members."""
        logger.info("Transformando memberships (contratos)...")
        
        sql = """
        INSERT INTO core.evo_member_memberships (
            member_membership_id, member_id, membership_id, membership_name,
            membership_renewed_id, sale_id, sale_date, start_date, end_date,
            cancel_date, cancel_date_on, cancel_creation_date, membership_status,
            value_next_month, original_value, next_charge, next_date_suspension,
            category_membership_id, loyalty_end_date, assessment_end_date,
            acceptance_date, num_members, fl_allow_locker, fl_additional_membership,
            allow_les_mills, allows_cancellation_by_app, signed_terms, limitless,
            weekly_limit, bioimpedance_amount, concluded_sessions, pending_sessions,
            scheduled_sessions, pending_repositions, repositions_total, bonus_sessions,
            number_suspension_times, max_suspension_days, minimum_suspension_days,
            disponible_suspension_days, disponible_suspension_times, days_left_to_freeze,
            contract_printing, freezes, sessions, _loaded_at, _updated_at
        )
        SELECT 
            (m->>'idMemberMembership')::BIGINT,
            member_id,
            (m->>'idMembership')::BIGINT,
            m->>'name',
            (m->>'idMemberMembershipRenewed')::BIGINT,
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
            _loaded_at,
            NOW()
        FROM stg_evo.members_raw,
        LATERAL jsonb_array_elements(
            CASE 
                WHEN raw_data->'memberships' IS NOT NULL 
                     AND jsonb_typeof(raw_data->'memberships') = 'array'
                THEN raw_data->'memberships'
                ELSE '[]'::jsonb
            END
        ) AS m
        WHERE (m->>'idMemberMembership') IS NOT NULL
        ON CONFLICT (member_id, member_membership_id) DO UPDATE SET
            membership_id = EXCLUDED.membership_id,
            membership_name = EXCLUDED.membership_name,
            membership_renewed_id = EXCLUDED.membership_renewed_id,
            sale_id = EXCLUDED.sale_id,
            sale_date = EXCLUDED.sale_date,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            cancel_date = EXCLUDED.cancel_date,
            cancel_date_on = EXCLUDED.cancel_date_on,
            cancel_creation_date = EXCLUDED.cancel_creation_date,
            membership_status = EXCLUDED.membership_status,
            value_next_month = EXCLUDED.value_next_month,
            original_value = EXCLUDED.original_value,
            next_charge = EXCLUDED.next_charge,
            next_date_suspension = EXCLUDED.next_date_suspension,
            category_membership_id = EXCLUDED.category_membership_id,
            loyalty_end_date = EXCLUDED.loyalty_end_date,
            assessment_end_date = EXCLUDED.assessment_end_date,
            acceptance_date = EXCLUDED.acceptance_date,
            num_members = EXCLUDED.num_members,
            fl_allow_locker = EXCLUDED.fl_allow_locker,
            fl_additional_membership = EXCLUDED.fl_additional_membership,
            allow_les_mills = EXCLUDED.allow_les_mills,
            allows_cancellation_by_app = EXCLUDED.allows_cancellation_by_app,
            signed_terms = EXCLUDED.signed_terms,
            limitless = EXCLUDED.limitless,
            weekly_limit = EXCLUDED.weekly_limit,
            bioimpedance_amount = EXCLUDED.bioimpedance_amount,
            concluded_sessions = EXCLUDED.concluded_sessions,
            pending_sessions = EXCLUDED.pending_sessions,
            scheduled_sessions = EXCLUDED.scheduled_sessions,
            pending_repositions = EXCLUDED.pending_repositions,
            repositions_total = EXCLUDED.repositions_total,
            bonus_sessions = EXCLUDED.bonus_sessions,
            number_suspension_times = EXCLUDED.number_suspension_times,
            max_suspension_days = EXCLUDED.max_suspension_days,
            minimum_suspension_days = EXCLUDED.minimum_suspension_days,
            disponible_suspension_days = EXCLUDED.disponible_suspension_days,
            disponible_suspension_times = EXCLUDED.disponible_suspension_times,
            days_left_to_freeze = EXCLUDED.days_left_to_freeze,
            contract_printing = EXCLUDED.contract_printing,
            freezes = EXCLUDED.freezes,
            sessions = EXCLUDED.sessions,
            _updated_at = NOW()
        """
        
        with self.conn.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
        
        self.conn.commit()
        logger.info(f"  Memberships: {count:,}")
        return count
    
    def transform_contacts(self) -> int:
        """Transforma contatos (telefones/emails) - 1:N com members."""
        logger.info("Transformando contatos...")
        
        sql = """
        INSERT INTO core.evo_member_contacts (
            phone_id, member_id, contact_type_id, contact_type, ddi, description, _loaded_at
        )
        SELECT 
            (c->>'idPhone')::BIGINT,
            member_id,
            (c->>'idContactType')::INT,
            c->>'contactType',
            c->>'ddi',
            c->>'description',
            _loaded_at
        FROM stg_evo.members_raw,
        LATERAL jsonb_array_elements(
            CASE 
                WHEN raw_data->'contacts' IS NOT NULL 
                     AND jsonb_typeof(raw_data->'contacts') = 'array'
                THEN raw_data->'contacts'
                ELSE '[]'::jsonb
            END
        ) AS c
        WHERE (c->>'idPhone') IS NOT NULL
        ON CONFLICT (member_id, phone_id) DO UPDATE SET
            contact_type_id = EXCLUDED.contact_type_id,
            contact_type = EXCLUDED.contact_type,
            ddi = EXCLUDED.ddi,
            description = EXCLUDED.description
        """
        
        with self.conn.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
        
        self.conn.commit()
        logger.info(f"  Contatos: {count:,}")
        return count
    
    def get_stats(self):
        stats = {}
        tables = [
            ("stg_evo.members_raw", "stg"),
            ("core.evo_members", "members"),
            ("core.evo_member_memberships", "memberships"),
            ("core.evo_member_contacts", "contacts"),
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
        logger.info("EVO MEMBERS - TRANSFORMAÇÃO CORE")
        logger.info("="*60)
        
        start = datetime.now()
        
        members = self.transform_members()
        memberships = self.transform_memberships()
        contacts = self.transform_contacts()
        
        elapsed = (datetime.now() - start).total_seconds()
        stats = self.get_stats()
        
        logger.info("="*60)
        logger.info("RESULTADO")
        logger.info("="*60)
        logger.info(f"Membros: {members:,}")
        logger.info(f"Memberships: {memberships:,}")
        logger.info(f"Contatos: {contacts:,}")
        logger.info(f"Tempo: {elapsed:.1f}s")
        
        return {
            "members": members, "memberships": memberships, "contacts": contacts,
            "elapsed": elapsed, "stats": stats
        }
    
    def close(self):
        if self.conn:
            self.conn.close()


def main():
    transformer = MembersTransformer()
    try:
        result = transformer.run()
        print(json.dumps(result, indent=2, default=str))
    finally:
        transformer.close()


if __name__ == "__main__":
    main()
