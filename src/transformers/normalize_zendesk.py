# -*- coding: utf-8 -*-
"""
Transformer: Zendesk STG (JSONB) -> CORE (tipado)

Normaliza os dados brutos do STG para tabelas tipadas no schema CORE.
Implementa UPSERT por ID real para idempotência.

CORREÇÕES v3:
- fetchall() + cursor separado (evita "no results to fetch")
- _safe_text() para campos que podem ser dict
- Schema alinhado com 04_core_tables.sql

Uso:
    python normalize_zendesk.py [--entities <list>] [--batch-size <N>]
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


class ZendeskCoreNormalizer:
    """Normaliza dados Zendesk do STG para CORE."""
    
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
        if value is None or value == '':
            return None
        if isinstance(value, dict):
            return self._safe_int(value.get('value') or value.get('id'))
        try:
            return int(value)
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
    
    def normalize_organizations(self, batch_size: int = 1000) -> int:
        """Normaliza organizations do STG para CORE."""
        self.logger.info("Normalizando organizations...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_zendesk.organizations_raw
            )
            SELECT payload, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.zd_organizations (
                organization_id, name, domain_names, group_id, shared_tickets,
                shared_comments, external_id, tags, organization_fields,
                created_at, updated_at,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (organization_id) DO UPDATE SET
                name = EXCLUDED.name,
                domain_names = EXCLUDED.domain_names,
                group_id = EXCLUDED.group_id,
                shared_tickets = EXCLUDED.shared_tickets,
                shared_comments = EXCLUDED.shared_comments,
                external_id = EXCLUDED.external_id,
                tags = EXCLUDED.tags,
                organization_fields = EXCLUDED.organization_fields,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql)
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Organizations: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        total = 0
        batch = []
        
        for row in rows:
            payload, run_id = row
            
            org_id = self._safe_int(payload.get('id'))
            if org_id is None:
                continue  # Pular registros sem ID
            
            record = (
                org_id,
                self._safe_text(payload.get('name')),
                self._safe_json(payload.get('domain_names')),
                self._safe_int(payload.get('group_id')),
                self._safe_bool(payload.get('shared_tickets')),
                self._safe_bool(payload.get('shared_comments')),
                self._safe_text(payload.get('external_id')),
                self._safe_json(payload.get('tags')),
                self._safe_json(payload.get('organization_fields')),
                self._safe_timestamp(payload.get('created_at')),
                self._safe_timestamp(payload.get('updated_at')),
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
    
    def normalize_users(self, batch_size: int = 1000) -> int:
        """Normaliza users do STG para CORE."""
        self.logger.info("Normalizando users...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_zendesk.users_raw
            )
            SELECT payload, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.zd_users (
                user_id, name, email, phone, role,
                organization_id, time_zone, locale,
                active, verified, suspended,
                tags, user_fields, external_id,
                alias, notes, details, default_group_id,
                only_private_comments, restricted_agent,
                shared, shared_agent, signature, ticket_restriction,
                created_at, updated_at, last_login_at,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (user_id) DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                role = EXCLUDED.role,
                organization_id = EXCLUDED.organization_id,
                time_zone = EXCLUDED.time_zone,
                locale = EXCLUDED.locale,
                active = EXCLUDED.active,
                verified = EXCLUDED.verified,
                suspended = EXCLUDED.suspended,
                tags = EXCLUDED.tags,
                user_fields = EXCLUDED.user_fields,
                external_id = EXCLUDED.external_id,
                alias = EXCLUDED.alias,
                notes = EXCLUDED.notes,
                details = EXCLUDED.details,
                default_group_id = EXCLUDED.default_group_id,
                only_private_comments = EXCLUDED.only_private_comments,
                restricted_agent = EXCLUDED.restricted_agent,
                shared = EXCLUDED.shared,
                shared_agent = EXCLUDED.shared_agent,
                signature = EXCLUDED.signature,
                ticket_restriction = EXCLUDED.ticket_restriction,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                last_login_at = EXCLUDED.last_login_at,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql)
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Users: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        total = 0
        batch = []
        
        for row in rows:
            payload, run_id = row
            
            user_id = self._safe_int(payload.get('id'))
            if user_id is None:
                continue
            
            record = (
                user_id,
                self._safe_text(payload.get('name')),
                self._safe_text(payload.get('email')),
                self._safe_text(payload.get('phone')),
                self._safe_text(payload.get('role')),
                self._safe_int(payload.get('organization_id')),
                self._safe_text(payload.get('time_zone')),
                self._safe_text(payload.get('locale')),
                self._safe_bool(payload.get('active')),
                self._safe_bool(payload.get('verified')),
                self._safe_bool(payload.get('suspended')),
                self._safe_json(payload.get('tags')),
                self._safe_json(payload.get('user_fields')),
                self._safe_text(payload.get('external_id')),
                self._safe_text(payload.get('alias')),
                self._safe_text(payload.get('notes')),
                self._safe_text(payload.get('details')),
                self._safe_int(payload.get('default_group_id')),
                self._safe_bool(payload.get('only_private_comments')),
                self._safe_bool(payload.get('restricted_agent')),
                self._safe_bool(payload.get('shared')),
                self._safe_bool(payload.get('shared_agent')),
                self._safe_text(payload.get('signature')),
                self._safe_text(payload.get('ticket_restriction')),
                self._safe_timestamp(payload.get('created_at')),
                self._safe_timestamp(payload.get('updated_at')),
                self._safe_timestamp(payload.get('last_login_at')),
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
        
        self.logger.info(f"  Users: {total} registros")
        return total
    
    def normalize_groups(self, batch_size: int = 1000) -> int:
        """Normaliza groups do STG para CORE."""
        self.logger.info("Normalizando groups...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_zendesk.groups_raw
            )
            SELECT payload, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.zd_groups (
                group_id, name, description, is_public, deleted,
                created_at, updated_at,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (group_id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                is_public = EXCLUDED.is_public,
                deleted = EXCLUDED.deleted,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql)
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Groups: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        batch = []
        
        for row in rows:
            payload, run_id = row
            
            group_id = self._safe_int(payload.get('id'))
            if group_id is None:
                continue
            
            record = (
                group_id,
                self._safe_text(payload.get('name')),
                self._safe_text(payload.get('description')),
                self._safe_bool(payload.get('is_public')),
                self._safe_bool(payload.get('deleted')),
                self._safe_timestamp(payload.get('created_at')),
                self._safe_timestamp(payload.get('updated_at')),
                run_id, now, now
            )
            batch.append(record)
        
        if batch:
            execute_values(write_cur, upsert_sql, batch)
        
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Groups: {len(batch)} registros")
        return len(batch)
    
    def normalize_ticket_fields(self, batch_size: int = 1000) -> int:
        """Normaliza ticket_fields do STG para CORE."""
        self.logger.info("Normalizando ticket_fields...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_zendesk.ticket_fields_raw
            )
            SELECT payload, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.zd_ticket_fields (
                field_id, type, title, description, position,
                active, required, collapsed_for_agents,
                regexp_for_validation, title_in_portal,
                visible_in_portal, editable_in_portal, required_in_portal,
                custom_field_options, system_field_options,
                created_at, updated_at,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (field_id) DO UPDATE SET
                type = EXCLUDED.type,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                position = EXCLUDED.position,
                active = EXCLUDED.active,
                required = EXCLUDED.required,
                collapsed_for_agents = EXCLUDED.collapsed_for_agents,
                regexp_for_validation = EXCLUDED.regexp_for_validation,
                title_in_portal = EXCLUDED.title_in_portal,
                visible_in_portal = EXCLUDED.visible_in_portal,
                editable_in_portal = EXCLUDED.editable_in_portal,
                required_in_portal = EXCLUDED.required_in_portal,
                custom_field_options = EXCLUDED.custom_field_options,
                system_field_options = EXCLUDED.system_field_options,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql)
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Ticket Fields: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        batch = []
        
        for row in rows:
            payload, run_id = row
            
            field_id = self._safe_int(payload.get('id'))
            if field_id is None:
                continue
            
            record = (
                field_id,
                self._safe_text(payload.get('type')),
                self._safe_text(payload.get('title')),
                self._safe_text(payload.get('description')),
                self._safe_int(payload.get('position')),
                self._safe_bool(payload.get('active')),
                self._safe_bool(payload.get('required')),
                self._safe_bool(payload.get('collapsed_for_agents')),
                self._safe_text(payload.get('regexp_for_validation')),
                self._safe_text(payload.get('title_in_portal')),
                self._safe_bool(payload.get('visible_in_portal')),
                self._safe_bool(payload.get('editable_in_portal')),
                self._safe_bool(payload.get('required_in_portal')),
                self._safe_json(payload.get('custom_field_options')),
                self._safe_json(payload.get('system_field_options')),
                self._safe_timestamp(payload.get('created_at')),
                self._safe_timestamp(payload.get('updated_at')),
                run_id, now, now
            )
            batch.append(record)
        
        if batch:
            execute_values(write_cur, upsert_sql, batch)
        
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Ticket Fields: {len(batch)} registros")
        return len(batch)
    
    def normalize_ticket_forms(self, batch_size: int = 1000) -> int:
        """Normaliza ticket_forms do STG para CORE."""
        self.logger.info("Normalizando ticket_forms...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_zendesk.ticket_forms_raw
            )
            SELECT payload, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.zd_ticket_forms (
                form_id, name, display_name, position,
                active, default_form, end_user_visible,
                in_all_brands, restricted_brand_ids, ticket_field_ids,
                created_at, updated_at,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (form_id) DO UPDATE SET
                name = EXCLUDED.name,
                display_name = EXCLUDED.display_name,
                position = EXCLUDED.position,
                active = EXCLUDED.active,
                default_form = EXCLUDED.default_form,
                end_user_visible = EXCLUDED.end_user_visible,
                in_all_brands = EXCLUDED.in_all_brands,
                restricted_brand_ids = EXCLUDED.restricted_brand_ids,
                ticket_field_ids = EXCLUDED.ticket_field_ids,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql)
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Ticket Forms: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        batch = []
        
        for row in rows:
            payload, run_id = row
            
            form_id = self._safe_int(payload.get('id'))
            if form_id is None:
                continue
            
            record = (
                form_id,
                self._safe_text(payload.get('name')),
                self._safe_text(payload.get('display_name')),
                self._safe_int(payload.get('position')),
                self._safe_bool(payload.get('active')),
                self._safe_bool(payload.get('default')),
                self._safe_bool(payload.get('end_user_visible')),
                self._safe_bool(payload.get('in_all_brands')),
                self._safe_json(payload.get('restricted_brand_ids')),
                self._safe_json(payload.get('ticket_field_ids')),
                self._safe_timestamp(payload.get('created_at')),
                self._safe_timestamp(payload.get('updated_at')),
                run_id, now, now
            )
            batch.append(record)
        
        if batch:
            execute_values(write_cur, upsert_sql, batch)
        
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Ticket Forms: {len(batch)} registros")
        return len(batch)
    
    def normalize_tickets(self, batch_size: int = 500) -> int:
        """Normaliza tickets do STG para CORE."""
        self.logger.info("Normalizando tickets...")
        
        select_sql = """
            WITH ranked AS (
                SELECT payload, run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY (payload->>'id')::bigint 
                        ORDER BY loaded_at DESC
                    ) as rn
                FROM stg_zendesk.tickets_raw
            )
            SELECT payload, run_id FROM ranked WHERE rn = 1
        """
        
        upsert_sql = """
            INSERT INTO core.zd_tickets (
                ticket_id, subject, description, status, priority, type,
                requester_id, submitter_id, assignee_id, organization_id, group_id,
                brand_id, ticket_form_id, external_id,
                via_channel, via_source,
                is_public, has_incidents, allow_channelback, allow_attachments,
                tags, custom_fields,
                created_at, updated_at,
                _source_run_id, _loaded_at, _updated_at
            ) VALUES %s
            ON CONFLICT (ticket_id) DO UPDATE SET
                subject = EXCLUDED.subject,
                description = EXCLUDED.description,
                status = EXCLUDED.status,
                priority = EXCLUDED.priority,
                type = EXCLUDED.type,
                requester_id = EXCLUDED.requester_id,
                submitter_id = EXCLUDED.submitter_id,
                assignee_id = EXCLUDED.assignee_id,
                organization_id = EXCLUDED.organization_id,
                group_id = EXCLUDED.group_id,
                brand_id = EXCLUDED.brand_id,
                ticket_form_id = EXCLUDED.ticket_form_id,
                external_id = EXCLUDED.external_id,
                via_channel = EXCLUDED.via_channel,
                via_source = EXCLUDED.via_source,
                is_public = EXCLUDED.is_public,
                has_incidents = EXCLUDED.has_incidents,
                allow_channelback = EXCLUDED.allow_channelback,
                allow_attachments = EXCLUDED.allow_attachments,
                tags = EXCLUDED.tags,
                custom_fields = EXCLUDED.custom_fields,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                _source_run_id = EXCLUDED._source_run_id,
                _updated_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql)
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Tickets: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        total = 0
        batch = []
        
        for row in rows:
            payload, run_id = row
            
            ticket_id = self._safe_int(payload.get('id'))
            if ticket_id is None:
                continue
            
            via = payload.get('via') or {}
            via_channel = via.get('channel') if isinstance(via, dict) else None
            via_source = self._safe_json(via.get('source')) if isinstance(via, dict) else None
            
            record = (
                ticket_id,
                self._safe_text(payload.get('subject')),
                self._safe_text(payload.get('description')),
                self._safe_text(payload.get('status')),
                self._safe_text(payload.get('priority')),
                self._safe_text(payload.get('type')),
                self._safe_int(payload.get('requester_id')),
                self._safe_int(payload.get('submitter_id')),
                self._safe_int(payload.get('assignee_id')),
                self._safe_int(payload.get('organization_id')),
                self._safe_int(payload.get('group_id')),
                self._safe_int(payload.get('brand_id')),
                self._safe_int(payload.get('ticket_form_id')),
                self._safe_text(payload.get('external_id')),
                self._safe_text(via_channel),
                via_source,
                self._safe_bool(payload.get('is_public')),
                self._safe_bool(payload.get('has_incidents')),
                self._safe_bool(payload.get('allow_channelback')),
                self._safe_bool(payload.get('allow_attachments')),
                self._safe_json(payload.get('tags')),
                self._safe_json(payload.get('custom_fields')),
                self._safe_timestamp(payload.get('created_at')),
                self._safe_timestamp(payload.get('updated_at')),
                run_id, now, now
            )
            batch.append(record)
            
            if len(batch) >= batch_size:
                execute_values(write_cur, upsert_sql, batch)
                total += len(batch)
                self.logger.info(f"  Progress: {total} tickets...")
                batch = []
        
        if batch:
            execute_values(write_cur, upsert_sql, batch)
            total += len(batch)
        
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Tickets: {total} registros")
        return total
    
    def normalize_ticket_tags(self, batch_size: int = 1000) -> int:
        """Extrai tags dos tickets para tabela separada."""
        self.logger.info("Extraindo ticket_tags...")
        
        # DISTINCT para evitar duplicatas no batch
        select_sql = """
            SELECT DISTINCT
                (payload->>'id')::bigint as ticket_id,
                tag
            FROM stg_zendesk.tickets_raw,
                 jsonb_array_elements_text(payload->'tags') as tag
            WHERE payload->'tags' IS NOT NULL
              AND jsonb_array_length(payload->'tags') > 0
        """
        
        upsert_sql = """
            INSERT INTO core.zd_ticket_tags (ticket_id, tag, _loaded_at)
            VALUES %s
            ON CONFLICT (ticket_id, tag) DO NOTHING
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql)
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Ticket Tags: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        total = 0
        batch = []
        
        # Deduplicar em Python também para garantir
        seen = set()
        for row in rows:
            ticket_id, tag = row
            key = (ticket_id, tag)
            if ticket_id and tag and key not in seen:
                seen.add(key)
                batch.append((ticket_id, tag, now))
            
            if len(batch) >= batch_size:
                execute_values(write_cur, upsert_sql, batch)
                total += len(batch)
                batch = []
        
        if batch:
            execute_values(write_cur, upsert_sql, batch)
            total += len(batch)
        
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Ticket Tags: {total} registros")
        return total
    
    def normalize_ticket_custom_fields(self, batch_size: int = 1000) -> int:
        """Extrai custom_fields dos tickets para tabela separada."""
        self.logger.info("Extraindo ticket_custom_fields...")
        
        # Query com DISTINCT ON para evitar duplicatas no mesmo batch
        select_sql = """
            WITH expanded AS (
                SELECT 
                    (payload->>'id')::bigint as ticket_id,
                    (cf->>'id')::bigint as field_id,
                    cf->>'value' as value,
                    loaded_at
                FROM stg_zendesk.tickets_raw,
                     jsonb_array_elements(payload->'custom_fields') as cf
                WHERE payload->'custom_fields' IS NOT NULL
                  AND jsonb_array_length(payload->'custom_fields') > 0
                  AND cf->>'value' IS NOT NULL
                  AND cf->>'value' != ''
            )
            SELECT DISTINCT ON (ticket_id, field_id)
                ticket_id, field_id, value
            FROM expanded
            ORDER BY ticket_id, field_id, loaded_at DESC
        """
        
        upsert_sql = """
            INSERT INTO core.zd_ticket_custom_fields (ticket_id, field_id, value, _loaded_at)
            VALUES %s
            ON CONFLICT (ticket_id, field_id) DO UPDATE SET
                value = EXCLUDED.value,
                _loaded_at = NOW()
        """
        
        read_cur = self.conn.cursor()
        read_cur.execute(select_sql)
        rows = read_cur.fetchall()
        read_cur.close()
        
        if not rows:
            self.logger.info("  Ticket Custom Fields: 0 registros")
            return 0
        
        write_cur = self.conn.cursor()
        now = datetime.now(timezone.utc)
        total = 0
        batch = []
        
        for row in rows:
            ticket_id, field_id, value = row
            if ticket_id and field_id:
                batch.append((ticket_id, field_id, value, now))
            
            if len(batch) >= batch_size:
                execute_values(write_cur, upsert_sql, batch)
                total += len(batch)
                batch = []
        
        if batch:
            execute_values(write_cur, upsert_sql, batch)
            total += len(batch)
        
        self.conn.commit()
        write_cur.close()
        
        self.logger.info(f"  Ticket Custom Fields: {total} registros")
        return total
    
    def normalize_all(self, batch_size: int = 1000) -> Dict[str, int]:
        """Normaliza todas as entidades na ordem correta."""
        results = {}
        
        # Dimensões primeiro
        results['organizations'] = self.normalize_organizations(batch_size)
        results['users'] = self.normalize_users(batch_size)
        results['groups'] = self.normalize_groups(batch_size)
        results['ticket_fields'] = self.normalize_ticket_fields(batch_size)
        results['ticket_forms'] = self.normalize_ticket_forms(batch_size)
        
        # Fatos
        results['tickets'] = self.normalize_tickets(batch_size // 2)
        
        # Tabelas derivadas
        results['ticket_tags'] = self.normalize_ticket_tags(batch_size)
        results['ticket_custom_fields'] = self.normalize_ticket_custom_fields(batch_size)
        
        return results


def main():
    parser = argparse.ArgumentParser(description="Normaliza Zendesk STG → CORE")
    parser.add_argument("--entities", nargs="+", help="Entidades específicas")
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    
    logger = RunLogger(run_name="normalize_zendesk", log_dir=PROJECT_ROOT / "logs")
    
    logger.info("=" * 60)
    logger.info("Zendesk CORE Normalizer (STG → CORE)")
    logger.info("=" * 60)
    
    try:
        normalizer = ZendeskCoreNormalizer(logger=logger)
        
        if args.entities:
            results = {}
            for entity in args.entities:
                method = getattr(normalizer, f"normalize_{entity}", None)
                if method:
                    results[entity] = method(args.batch_size)
                else:
                    logger.warning(f"Entidade desconhecida: {entity}")
        else:
            results = normalizer.normalize_all(args.batch_size)
        
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
