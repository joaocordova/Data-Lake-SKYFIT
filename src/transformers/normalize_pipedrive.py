# -*- coding: utf-8 -*-
"""
Transformer: Pipedrive STG (JSONB) -> CORE (tipado)

Normaliza os dados brutos do STG para tabelas tipadas no schema CORE.
Implementa UPSERT por (id, scope) para idempotência.

CORREÇÕES v3:
- fetchall() + cursor separado (evita "no results to fetch")
- _safe_text() para TODOS campos que podem ser dict (evita "can't adapt type 'dict'")

Uso:
    python normalize_pipedrive.py [--scopes <list>] [--entities <list>] [--batch-size <N>]
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import execute_values

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import postgres
from src.common.logging_config import RunLogger


DEFAULT_SCOPES = ["comercial", "expansao"]


class PipedriveCoreNormalizer:
    """Normaliza dados Pipedrive do STG para CORE."""
    
    def __init__(self, logger: RunLogger):
        self.logger = logger
        self._conn = None
    
    @property
    def conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=postgres.HOST,
                port=postgres.PORT,
                dbname=postgres.DATABASE,
                user=postgres.USER,
                password=postgres.PASSWORD,
                sslmode=postgres.SSLMODE
            )
        return self._conn
    
    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
    
    # =========================================================================
    # Safe Type Conversion Helpers
    # =========================================================================
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """Converte para int, extraindo de dict se necessário."""
        if value is None:
            return None
        if isinstance(value, dict):
            return self._safe_int(value.get('value') or value.get('id'))
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None or value == '':
            return None
        if isinstance(value, dict):
            return self._safe_float(value.get('value'))
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_bool(self, value: Any) -> Optional[bool]:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes')
        return bool(value)
    
    def _safe_timestamp(self, value: Any) -> Optional[str]:
        if value is None or value == '':
            return None
        return str(value)
    
    def _safe_json(self, value: Any) -> Optional[str]:
        """Converte dict/list para JSON string."""
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return None
    
    def _safe_text(self, value: Any) -> Optional[str]:
        """Converte qualquer valor para texto, serializando dict/list como JSON."""
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)
    
    # =========================================================================
    # Normalize Methods
    # =========================================================================
    
    def normalize_pipelines(self, scopes: List[str], batch_size: int = 1000) -> int:
        """Normaliza pipelines do STG para CORE."""
        self.logger.info("Normalizando pipelines...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, scope, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY scope, (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_pipedrive.pipelines_raw
                WHERE scope = ANY(%s)
            )
            SELECT payload, scope, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.pd_pipelines (
                pipeline_id, scope, name, url_title, order_nr,
                active, deal_probability, add_time, update_time,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (pipeline_id, scope) DO UPDATE SET
                name = EXCLUDED.name,
                url_title = EXCLUDED.url_title,
                order_nr = EXCLUDED.order_nr,
                active = EXCLUDED.active,
                deal_probability = EXCLUDED.deal_probability,
                add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql, (scopes,))
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Pipelines: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        batch = []
        
        for row in rows:
            payload, scope, run_id = row
            record = (
                self._safe_int(payload.get('id')),
                scope,
                self._safe_text(payload.get('name')),
                self._safe_text(payload.get('url_title')),
                self._safe_int(payload.get('order_nr')),
                self._safe_bool(payload.get('active')),
                self._safe_bool(payload.get('deal_probability')),
                self._safe_timestamp(payload.get('add_time')),
                self._safe_timestamp(payload.get('update_time')),
                run_id, now, now
            )
            batch.append(record)
        
        execute_values(write_cur, upsert_sql, batch)
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Pipelines: {len(batch)} registros")
        return len(batch)
    
    def normalize_stages(self, scopes: List[str], batch_size: int = 1000) -> int:
        """Normaliza stages do STG para CORE."""
        self.logger.info("Normalizando stages...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, scope, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY scope, (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_pipedrive.stages_raw
                WHERE scope = ANY(%s)
            )
            SELECT payload, scope, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.pd_stages (
                stage_id, scope, pipeline_id, name, order_nr,
                active_flag, deal_probability, rotten_flag, rotten_days,
                add_time, update_time,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (stage_id, scope) DO UPDATE SET
                pipeline_id = EXCLUDED.pipeline_id,
                name = EXCLUDED.name,
                order_nr = EXCLUDED.order_nr,
                active_flag = EXCLUDED.active_flag,
                deal_probability = EXCLUDED.deal_probability,
                rotten_flag = EXCLUDED.rotten_flag,
                rotten_days = EXCLUDED.rotten_days,
                add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql, (scopes,))
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Stages: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        batch = []
        
        for row in rows:
            payload, scope, run_id = row
            record = (
                self._safe_int(payload.get('id')),
                scope,
                self._safe_int(payload.get('pipeline_id')),
                self._safe_text(payload.get('name')),
                self._safe_int(payload.get('order_nr')),
                self._safe_bool(payload.get('active_flag')),
                self._safe_int(payload.get('deal_probability')),
                self._safe_bool(payload.get('rotten_flag')),
                self._safe_int(payload.get('rotten_days')),
                self._safe_timestamp(payload.get('add_time')),
                self._safe_timestamp(payload.get('update_time')),
                run_id, now, now
            )
            batch.append(record)
        
        execute_values(write_cur, upsert_sql, batch)
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Stages: {len(batch)} registros")
        return len(batch)
    
    def normalize_users(self, scopes: List[str], batch_size: int = 1000) -> int:
        """Normaliza users do STG para CORE."""
        self.logger.info("Normalizando users...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, scope, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY scope, (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_pipedrive.users_raw
                WHERE scope = ANY(%s)
            )
            SELECT payload, scope, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.pd_users (
                user_id, scope, name, email, active_flag, is_admin,
                role_id, icon_url, timezone_name, timezone_offset,
                phone, created, modified,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (user_id, scope) DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                active_flag = EXCLUDED.active_flag,
                is_admin = EXCLUDED.is_admin,
                role_id = EXCLUDED.role_id,
                icon_url = EXCLUDED.icon_url,
                timezone_name = EXCLUDED.timezone_name,
                timezone_offset = EXCLUDED.timezone_offset,
                phone = EXCLUDED.phone,
                created = EXCLUDED.created,
                modified = EXCLUDED.modified,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql, (scopes,))
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Users: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        batch = []
        
        for row in rows:
            payload, scope, run_id = row
            record = (
                self._safe_int(payload.get('id')),
                scope,
                self._safe_text(payload.get('name')),
                self._safe_text(payload.get('email')),
                self._safe_bool(payload.get('active_flag')),
                self._safe_bool(payload.get('is_admin')),
                self._safe_int(payload.get('role_id')),
                self._safe_text(payload.get('icon_url')),
                self._safe_text(payload.get('timezone_name')),
                self._safe_text(payload.get('timezone_offset')),
                self._safe_text(payload.get('phone')),
                self._safe_timestamp(payload.get('created')),
                self._safe_timestamp(payload.get('modified')),
                run_id, now, now
            )
            batch.append(record)
        
        execute_values(write_cur, upsert_sql, batch)
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Users: {len(batch)} registros")
        return len(batch)
    
    def normalize_organizations(self, scopes: List[str], batch_size: int = 1000) -> int:
        """Normaliza organizations do STG para CORE."""
        self.logger.info("Normalizando organizations...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, scope, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY scope, (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_pipedrive.organizations_raw
                WHERE scope = ANY(%s)
            )
            SELECT payload, scope, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.pd_organizations (
                org_id, scope, name, owner_id,
                address, address_locality, address_country, address_postal_code,
                cc_email, active_flag,
                people_count, open_deals_count, won_deals_count, lost_deals_count,
                add_time, update_time, custom_fields,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (org_id, scope) DO UPDATE SET
                name = EXCLUDED.name,
                owner_id = EXCLUDED.owner_id,
                address = EXCLUDED.address,
                address_locality = EXCLUDED.address_locality,
                address_country = EXCLUDED.address_country,
                address_postal_code = EXCLUDED.address_postal_code,
                cc_email = EXCLUDED.cc_email,
                active_flag = EXCLUDED.active_flag,
                people_count = EXCLUDED.people_count,
                open_deals_count = EXCLUDED.open_deals_count,
                won_deals_count = EXCLUDED.won_deals_count,
                lost_deals_count = EXCLUDED.lost_deals_count,
                add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time,
                custom_fields = EXCLUDED.custom_fields,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql, (scopes,))
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Organizations: 0 registros")
            return 0
        
        standard_fields = {
            'id', 'name', 'owner_id', 'owner_name',
            'address', 'address_locality', 'address_country', 'address_postal_code',
            'cc_email', 'active_flag',
            'people_count', 'open_deals_count', 'won_deals_count', 'lost_deals_count',
            'add_time', 'update_time', 'visible_to', 'next_activity_date',
            'last_activity_date', 'activities_count', 'done_activities_count',
            'undone_activities_count', 'files_count', 'notes_count', 'followers_count',
            'email_messages_count', 'picture_id', 'related_closed_deals_count',
            'related_lost_deals_count', 'related_open_deals_count', 'related_won_deals_count',
            'label', 'address_subpremise', 'address_street_number', 'address_route',
            'address_sublocality', 'address_admin_area_level_1', 'address_admin_area_level_2',
            'address_formatted_address'
        }
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        total = 0
        batch = []
        
        for row in rows:
            payload, scope, run_id = row
            
            custom_fields = {
                k: v for k, v in payload.items()
                if k not in standard_fields and not k.startswith('_')
            }
            
            record = (
                self._safe_int(payload.get('id')),
                scope,
                self._safe_text(payload.get('name')),
                self._safe_int(payload.get('owner_id')),
                self._safe_text(payload.get('address')),
                self._safe_text(payload.get('address_locality')),
                self._safe_text(payload.get('address_country')),
                self._safe_text(payload.get('address_postal_code')),
                self._safe_text(payload.get('cc_email')),
                self._safe_bool(payload.get('active_flag')),
                self._safe_int(payload.get('people_count')),
                self._safe_int(payload.get('open_deals_count')),
                self._safe_int(payload.get('won_deals_count')),
                self._safe_int(payload.get('lost_deals_count')),
                self._safe_timestamp(payload.get('add_time')),
                self._safe_timestamp(payload.get('update_time')),
                self._safe_json(custom_fields) if custom_fields else None,
                run_id, now, now
            )
            batch.append(record)
            
            if len(batch) >= batch_size:
                execute_values(write_cur, upsert_sql, batch)
                total += len(batch)
                batch = []
        
        if batch:
            execute_values(write_cur, upsert_sql, batch)
            total += len(batch)
        
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Organizations: {total} registros")
        return total
    
    def normalize_persons(self, scopes: List[str], batch_size: int = 1000) -> int:
        """Normaliza persons do STG para CORE."""
        self.logger.info("Normalizando persons...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, scope, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY scope, (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_pipedrive.persons_raw
                WHERE scope = ANY(%s)
            )
            SELECT payload, scope, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.pd_persons (
                person_id, scope, name, first_name, last_name,
                org_id, owner_id,
                primary_email, emails, primary_phone, phones,
                visible_to, active_flag,
                open_deals_count, related_open_deals_count,
                closed_deals_count, related_closed_deals_count,
                won_deals_count, related_won_deals_count,
                lost_deals_count, related_lost_deals_count,
                add_time, update_time, custom_fields,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (person_id, scope) DO UPDATE SET
                name = EXCLUDED.name,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                org_id = EXCLUDED.org_id,
                owner_id = EXCLUDED.owner_id,
                primary_email = EXCLUDED.primary_email,
                emails = EXCLUDED.emails,
                primary_phone = EXCLUDED.primary_phone,
                phones = EXCLUDED.phones,
                visible_to = EXCLUDED.visible_to,
                active_flag = EXCLUDED.active_flag,
                open_deals_count = EXCLUDED.open_deals_count,
                related_open_deals_count = EXCLUDED.related_open_deals_count,
                closed_deals_count = EXCLUDED.closed_deals_count,
                related_closed_deals_count = EXCLUDED.related_closed_deals_count,
                won_deals_count = EXCLUDED.won_deals_count,
                related_won_deals_count = EXCLUDED.related_won_deals_count,
                lost_deals_count = EXCLUDED.lost_deals_count,
                related_lost_deals_count = EXCLUDED.related_lost_deals_count,
                add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time,
                custom_fields = EXCLUDED.custom_fields,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql, (scopes,))
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Persons: 0 registros")
            return 0
        
        standard_fields = {
            'id', 'name', 'first_name', 'last_name',
            'org_id', 'owner_id', 'email', 'phone',
            'visible_to', 'active_flag',
            'open_deals_count', 'related_open_deals_count',
            'closed_deals_count', 'related_closed_deals_count',
            'won_deals_count', 'related_won_deals_count',
            'lost_deals_count', 'related_lost_deals_count',
            'add_time', 'update_time', 'activities_count', 'done_activities_count',
            'undone_activities_count', 'files_count', 'notes_count', 'followers_count',
            'email_messages_count', 'last_activity_date', 'next_activity_date',
            'picture_id', 'label', 'org_name', 'owner_name', 'cc_email',
            'primary_email', 'marketing_status'
        }
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        total = 0
        batch = []
        
        for row in rows:
            payload, scope, run_id = row
            
            # Extract primary email/phone
            emails = payload.get('email', []) or []
            phones = payload.get('phone', []) or []
            
            primary_email = None
            primary_phone = None
            
            if isinstance(emails, list) and emails:
                for e in emails:
                    if isinstance(e, dict):
                        if e.get('primary'):
                            primary_email = e.get('value')
                            break
                        elif not primary_email:
                            primary_email = e.get('value')
            
            if isinstance(phones, list) and phones:
                for p in phones:
                    if isinstance(p, dict):
                        if p.get('primary'):
                            primary_phone = p.get('value')
                            break
                        elif not primary_phone:
                            primary_phone = p.get('value')
            
            custom_fields = {
                k: v for k, v in payload.items()
                if k not in standard_fields and not k.startswith('_')
            }
            
            record = (
                self._safe_int(payload.get('id')),
                scope,
                self._safe_text(payload.get('name')),
                self._safe_text(payload.get('first_name')),
                self._safe_text(payload.get('last_name')),
                self._safe_int(payload.get('org_id')),
                self._safe_int(payload.get('owner_id')),
                primary_email,
                self._safe_json(emails) if emails else None,
                primary_phone,
                self._safe_json(phones) if phones else None,
                self._safe_int(payload.get('visible_to')),
                self._safe_bool(payload.get('active_flag')),
                self._safe_int(payload.get('open_deals_count')),
                self._safe_int(payload.get('related_open_deals_count')),
                self._safe_int(payload.get('closed_deals_count')),
                self._safe_int(payload.get('related_closed_deals_count')),
                self._safe_int(payload.get('won_deals_count')),
                self._safe_int(payload.get('related_won_deals_count')),
                self._safe_int(payload.get('lost_deals_count')),
                self._safe_int(payload.get('related_lost_deals_count')),
                self._safe_timestamp(payload.get('add_time')),
                self._safe_timestamp(payload.get('update_time')),
                self._safe_json(custom_fields) if custom_fields else None,
                run_id, now, now
            )
            batch.append(record)
            
            if len(batch) >= batch_size:
                execute_values(write_cur, upsert_sql, batch)
                total += len(batch)
                self.logger.info(f"  Progress: {total} persons...")
                batch = []
        
        if batch:
            execute_values(write_cur, upsert_sql, batch)
            total += len(batch)
        
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Persons: {total} registros")
        return total
    
    def normalize_deals(self, scopes: List[str], batch_size: int = 500) -> int:
        """Normaliza deals do STG para CORE."""
        self.logger.info("Normalizando deals...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, scope, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY scope, (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_pipedrive.deals_raw
                WHERE scope = ANY(%s)
            )
            SELECT payload, scope, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.pd_deals (
                deal_id, scope, title, value, currency, status,
                person_id, org_id, user_id, pipeline_id, stage_id,
                expected_close_date, probability,
                won_time, lost_time, close_time,
                add_time, update_time,
                stage_change_time, lost_reason,
                visible_to, activities_count, done_activities_count,
                undone_activities_count, files_count, notes_count,
                followers_count, email_messages_count,
                products_count, next_activity_date, last_activity_date,
                origin, channel, custom_fields,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (deal_id, scope) DO UPDATE SET
                title = EXCLUDED.title,
                value = EXCLUDED.value,
                currency = EXCLUDED.currency,
                status = EXCLUDED.status,
                person_id = EXCLUDED.person_id,
                org_id = EXCLUDED.org_id,
                user_id = EXCLUDED.user_id,
                pipeline_id = EXCLUDED.pipeline_id,
                stage_id = EXCLUDED.stage_id,
                expected_close_date = EXCLUDED.expected_close_date,
                probability = EXCLUDED.probability,
                won_time = EXCLUDED.won_time,
                lost_time = EXCLUDED.lost_time,
                close_time = EXCLUDED.close_time,
                add_time = EXCLUDED.add_time,
                update_time = EXCLUDED.update_time,
                stage_change_time = EXCLUDED.stage_change_time,
                lost_reason = EXCLUDED.lost_reason,
                visible_to = EXCLUDED.visible_to,
                activities_count = EXCLUDED.activities_count,
                done_activities_count = EXCLUDED.done_activities_count,
                undone_activities_count = EXCLUDED.undone_activities_count,
                files_count = EXCLUDED.files_count,
                notes_count = EXCLUDED.notes_count,
                followers_count = EXCLUDED.followers_count,
                email_messages_count = EXCLUDED.email_messages_count,
                products_count = EXCLUDED.products_count,
                next_activity_date = EXCLUDED.next_activity_date,
                last_activity_date = EXCLUDED.last_activity_date,
                origin = EXCLUDED.origin,
                channel = EXCLUDED.channel,
                custom_fields = EXCLUDED.custom_fields,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql, (scopes,))
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Deals: 0 registros")
            return 0
        
        standard_fields = {
            'id', 'title', 'value', 'currency', 'status',
            'person_id', 'org_id', 'user_id', 'pipeline_id', 'stage_id',
            'expected_close_date', 'probability',
            'won_time', 'lost_time', 'close_time',
            'add_time', 'update_time', 'stage_change_time',
            'lost_reason', 'visible_to',
            'activities_count', 'done_activities_count', 'undone_activities_count',
            'files_count', 'notes_count', 'followers_count', 'email_messages_count',
            'products_count', 'next_activity_date', 'last_activity_date',
            'origin', 'channel', 'creator_user_id', 'person_name', 'org_name',
            'stage_order_nr', 'owner_name', 'formatted_value', 'weighted_value',
            'formatted_weighted_value', 'rotten_time', 'cc_email', 'org_hidden',
            'person_hidden', 'next_activity_subject', 'next_activity_type',
            'next_activity_duration', 'next_activity_note', 'next_activity_id',
            'next_activity_time', 'last_activity_id', 'last_incoming_mail_time',
            'last_outgoing_mail_time', 'label', 'local_won_date', 'local_lost_date',
            'local_close_date', 'first_won_time', 'deleted', 'renewal_type',
            'stage_id_before_last_stage_change', 'acv', 'arr', 'mrr'
        }
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        total = 0
        batch = []
        
        for row in rows:
            payload, scope, run_id = row
            
            custom_fields = {
                k: v for k, v in payload.items()
                if k not in standard_fields and not k.startswith('_')
            }
            
            record = (
                self._safe_int(payload.get('id')),
                scope,
                self._safe_text(payload.get('title')),
                self._safe_float(payload.get('value')),
                self._safe_text(payload.get('currency')),
                self._safe_text(payload.get('status')),
                self._safe_int(payload.get('person_id')),
                self._safe_int(payload.get('org_id')),
                self._safe_int(payload.get('user_id')),
                self._safe_int(payload.get('pipeline_id')),
                self._safe_int(payload.get('stage_id')),
                self._safe_text(payload.get('expected_close_date')),
                self._safe_int(payload.get('probability')),
                self._safe_timestamp(payload.get('won_time')),
                self._safe_timestamp(payload.get('lost_time')),
                self._safe_timestamp(payload.get('close_time')),
                self._safe_timestamp(payload.get('add_time')),
                self._safe_timestamp(payload.get('update_time')),
                self._safe_timestamp(payload.get('stage_change_time')),
                self._safe_text(payload.get('lost_reason')),
                self._safe_int(payload.get('visible_to')),
                self._safe_int(payload.get('activities_count')),
                self._safe_int(payload.get('done_activities_count')),
                self._safe_int(payload.get('undone_activities_count')),
                self._safe_int(payload.get('files_count')),
                self._safe_int(payload.get('notes_count')),
                self._safe_int(payload.get('followers_count')),
                self._safe_int(payload.get('email_messages_count')),
                self._safe_int(payload.get('products_count')),
                self._safe_text(payload.get('next_activity_date')),
                self._safe_text(payload.get('last_activity_date')),
                self._safe_text(payload.get('origin')),
                self._safe_int(payload.get('channel')),
                self._safe_json(custom_fields) if custom_fields else None,
                run_id, now, now
            )
            batch.append(record)
            
            if len(batch) >= batch_size:
                execute_values(write_cur, upsert_sql, batch)
                total += len(batch)
                self.logger.info(f"  Progress: {total} deals...")
                batch = []
        
        if batch:
            execute_values(write_cur, upsert_sql, batch)
            total += len(batch)
        
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Deals: {total} registros")
        return total
    
    def normalize_activities(self, scopes: List[str], batch_size: int = 500) -> int:
        """Normaliza activities do STG para CORE."""
        self.logger.info("Normalizando activities...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, scope, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY scope, (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_pipedrive.activities_raw
                WHERE scope = ANY(%s)
            )
            SELECT payload, scope, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.pd_activities (
                activity_id, scope, type, subject, note,
                done, busy_flag,
                user_id, deal_id, person_id, org_id, lead_id, project_id,
                due_date, due_time, duration,
                add_time, marked_as_done_time, update_time,
                location, location_formatted_address,
                participants, attendees,
                conference_meeting_client, conference_meeting_url, conference_meeting_id,
                public_description, active_flag,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (activity_id, scope) DO UPDATE SET
                type = EXCLUDED.type,
                subject = EXCLUDED.subject,
                note = EXCLUDED.note,
                done = EXCLUDED.done,
                busy_flag = EXCLUDED.busy_flag,
                user_id = EXCLUDED.user_id,
                deal_id = EXCLUDED.deal_id,
                person_id = EXCLUDED.person_id,
                org_id = EXCLUDED.org_id,
                lead_id = EXCLUDED.lead_id,
                project_id = EXCLUDED.project_id,
                due_date = EXCLUDED.due_date,
                due_time = EXCLUDED.due_time,
                duration = EXCLUDED.duration,
                add_time = EXCLUDED.add_time,
                marked_as_done_time = EXCLUDED.marked_as_done_time,
                update_time = EXCLUDED.update_time,
                location = EXCLUDED.location,
                location_formatted_address = EXCLUDED.location_formatted_address,
                participants = EXCLUDED.participants,
                attendees = EXCLUDED.attendees,
                conference_meeting_client = EXCLUDED.conference_meeting_client,
                conference_meeting_url = EXCLUDED.conference_meeting_url,
                conference_meeting_id = EXCLUDED.conference_meeting_id,
                public_description = EXCLUDED.public_description,
                active_flag = EXCLUDED.active_flag,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql, (scopes,))
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Activities: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        total = 0
        batch = []
        
        for row in rows:
            payload, scope, run_id = row
            
            # IMPORTANTE: Todos os campos que podem ser dict precisam de _safe_text
            record = (
                self._safe_int(payload.get('id')),
                scope,
                self._safe_text(payload.get('type')),
                self._safe_text(payload.get('subject')),
                self._safe_text(payload.get('note')),
                self._safe_bool(payload.get('done')),
                self._safe_bool(payload.get('busy_flag')),
                self._safe_int(payload.get('user_id')),
                self._safe_int(payload.get('deal_id')),
                self._safe_int(payload.get('person_id')),
                self._safe_int(payload.get('org_id')),
                self._safe_text(payload.get('lead_id')),
                self._safe_int(payload.get('project_id')),
                self._safe_text(payload.get('due_date')),
                self._safe_text(payload.get('due_time')),       # Pode ser dict!
                self._safe_text(payload.get('duration')),       # Pode ser dict!
                self._safe_timestamp(payload.get('add_time')),
                self._safe_timestamp(payload.get('marked_as_done_time')),
                self._safe_timestamp(payload.get('update_time')),
                self._safe_text(payload.get('location')),       # Pode ser dict!
                self._safe_text(payload.get('location_formatted_address')),
                self._safe_json(payload.get('participants')),
                self._safe_json(payload.get('attendees')),
                self._safe_text(payload.get('conference_meeting_client')),
                self._safe_text(payload.get('conference_meeting_url')),
                self._safe_text(payload.get('conference_meeting_id')),
                self._safe_text(payload.get('public_description')),
                self._safe_bool(payload.get('active_flag')),
                run_id, now, now
            )
            batch.append(record)
            
            if len(batch) >= batch_size:
                execute_values(write_cur, upsert_sql, batch)
                total += len(batch)
                self.logger.info(f"  Progress: {total} activities...")
                batch = []
        
        if batch:
            execute_values(write_cur, upsert_sql, batch)
            total += len(batch)
        
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Activities: {total} registros")
        return total
    
    def normalize_all(self, scopes: List[str], batch_size: int = 1000) -> Dict[str, int]:
        """Normaliza todas as entidades na ordem correta."""
        results = {}
        
        # Dimensões primeiro (ordem importa para FK)
        results['pipelines'] = self.normalize_pipelines(scopes, batch_size)
        results['stages'] = self.normalize_stages(scopes, batch_size)
        results['users'] = self.normalize_users(scopes, batch_size)
        results['organizations'] = self.normalize_organizations(scopes, batch_size)
        results['persons'] = self.normalize_persons(scopes, batch_size)
        
        # Fatos (batch menor)
        results['deals'] = self.normalize_deals(scopes, batch_size // 2)
        results['activities'] = self.normalize_activities(scopes, batch_size // 2)
        
        return results


def main():
    parser = argparse.ArgumentParser(description="Normaliza Pipedrive STG → CORE")
    parser.add_argument("--scopes", nargs="+", default=DEFAULT_SCOPES)
    parser.add_argument("--entities", nargs="+", help="Entidades específicas")
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    
    logger = RunLogger(run_name="normalize_pipedrive", log_dir=PROJECT_ROOT / "logs")
    
    logger.info("=" * 60)
    logger.info("Pipedrive CORE Normalizer (STG → CORE)")
    logger.info("=" * 60)
    
    try:
        normalizer = PipedriveCoreNormalizer(logger=logger)
        
        if args.entities:
            results = {}
            for entity in args.entities:
                method = getattr(normalizer, f"normalize_{entity}", None)
                if method:
                    results[entity] = method(args.scopes, args.batch_size)
                else:
                    logger.warning(f"Entidade desconhecida: {entity}")
        else:
            results = normalizer.normalize_all(args.scopes, args.batch_size)
        
        for entity, count in results.items():
            logger.set_metric(f"{entity}_count", count)
        logger.set_metric("total_records", sum(results.values()))
        normalizer.close()
        
        logger.info("=" * 60)
        logger.info("Normalização Completa")
        for entity, count in results.items():
            logger.info(f"  {entity}: {count}")
        logger.info("=" * 60)
        
        print(json.dumps(results, indent=2))
        
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        summary = logger.log_summary()
        if summary["status"] == "FAILED":
            sys.exit(1)


if __name__ == "__main__":
    main()
