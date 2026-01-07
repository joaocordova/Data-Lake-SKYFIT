# -*- coding: utf-8 -*-
"""
Loader: Zendesk Bronze (ADLS) -> STG (PostgreSQL)

Carrega todos os arquivos .jsonl.gz de um run do Bronze para as tabelas
stg_zendesk.*_raw no PostgreSQL com rastreabilidade completa.

Uso:
    python load_zendesk_stg.py [--run-id <ID>] [--entities <list>]
    
    Se --run-id não for fornecido, busca o run mais recente.
"""
import argparse
import gzip
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json

# Adiciona o diretório raiz ao path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import azure_storage, postgres
from src.common.lake import LakeClient
from src.common.logging_config import RunLogger


# Mapeamento entidade -> tabela STG
ENTITY_TABLE_MAP = {
    "tickets": "tickets_raw",
    "users": "users_raw",
    "organizations": "organizations_raw",
    "groups": "groups_raw",
    "ticket_fields": "ticket_fields_raw",
    "ticket_forms": "ticket_forms_raw",
}

# Schema do STG
STG_SCHEMA = "stg_zendesk"


class ZendeskStgLoader:
    """Carrega dados do Bronze Zendesk para STG no PostgreSQL."""
    
    def __init__(self, lake: LakeClient, logger: RunLogger):
        self.lake = lake
        self.logger = logger
        self._conn = None
    
    @property
    def conn(self):
        """Conexão lazy com o PostgreSQL."""
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
        """Fecha conexão."""
        if self._conn and not self._conn.closed:
            self._conn.close()
    
    def find_latest_run_id(self, scope: str = "support") -> Optional[str]:
        """Encontra o run_id mais recente para Zendesk."""
        prefix = "_meta/zendesk/runs/"
        manifests = []
        
        for blob_name in self.lake.list_blobs(prefix):
            if blob_name.endswith("/manifest.json"):
                # Extrai run_id do path
                parts = blob_name.split("/")
                for part in parts:
                    if part.startswith("run_id="):
                        run_id = part.replace("run_id=", "")
                        manifests.append((run_id, blob_name))
                        break
        
        if not manifests:
            return None
        
        # Ordena por run_id (formato timestamp YYYYMMDDTHHMMSSZ)
        manifests.sort(key=lambda x: x[0], reverse=True)
        return manifests[0][0]
    
    def get_manifest(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Obtém o manifest de um run."""
        path = f"_meta/zendesk/runs/run_id={run_id}/manifest.json"
        return self.lake.read_json(path)
    
    def list_bronze_parts(self, entity: str, scope: str, 
                          ingestion_date: str, run_id: str) -> List[str]:
        """Lista todos os parts de uma entidade no Bronze."""
        prefix = (
            f"bronze/zendesk/scope={scope}/entity={entity}/"
            f"ingestion_date={ingestion_date}/run_id={run_id}/"
        )
        
        parts = []
        for blob_name in self.lake.list_blobs(prefix):
            if blob_name.endswith(".jsonl.gz"):
                parts.append(blob_name)
        
        parts.sort()  # Garante ordem por part number
        return parts
    
    def read_jsonl_gz(self, blob_path: str) -> List[Dict[str, Any]]:
        """Lê um arquivo .jsonl.gz do ADLS."""
        bc = self.lake.container.get_blob_client(blob_path)
        compressed = bc.download_blob().readall()
        decompressed = gzip.decompress(compressed).decode("utf-8")
        
        records = []
        for line in decompressed.strip().split("\n"):
            if line.strip():
                records.append(json.loads(line))
        return records
    
    def load_entity(self, entity: str, scope: str, 
                    ingestion_date: str, run_id: str,
                    batch_size: int = 500) -> Tuple[int, int]:
        """
        Carrega uma entidade do Bronze para o STG.
        
        Returns:
            Tuple (total_records, parts_processed)
        """
        table_name = ENTITY_TABLE_MAP.get(entity)
        if not table_name:
            self.logger.warning(f"Entidade desconhecida: {entity}")
            return 0, 0
        
        parts = self.list_bronze_parts(entity, scope, ingestion_date, run_id)
        if not parts:
            self.logger.info(f"Nenhum part encontrado para {entity}")
            return 0, 0
        
        self.logger.info(f"Carregando {entity}: {len(parts)} parts")
        
        # SQL de inserção com ON CONFLICT para idempotência
        insert_sql = f"""
            INSERT INTO {STG_SCHEMA}.{table_name} 
                (payload, source_blob_path, source_line_no, run_id, ingestion_date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source_blob_path, source_line_no) 
            DO UPDATE SET 
                payload = EXCLUDED.payload,
                run_id = EXCLUDED.run_id,
                loaded_at = NOW()
        """
        
        total_records = 0
        cursor = self.conn.cursor()
        
        try:
            for part_path in parts:
                records = self.read_jsonl_gz(part_path)
                
                batch = []
                for line_no, record in enumerate(records, start=1):
                    batch.append((
                        Json(record),
                        part_path,
                        line_no,
                        run_id,
                        ingestion_date
                    ))
                    
                    if len(batch) >= batch_size:
                        cursor.executemany(insert_sql, batch)
                        total_records += len(batch)
                        batch = []
                
                # Flush remaining
                if batch:
                    cursor.executemany(insert_sql, batch)
                    total_records += len(batch)
                
                self.conn.commit()
                self.logger.debug(f"  Part {part_path}: {len(records)} records")
            
        except Exception as e:
            self.conn.rollback()
            raise
        finally:
            cursor.close()
        
        return total_records, len(parts)
    
    def load_run(self, run_id: str, scope: str = "support",
                 entities: List[str] = None) -> Dict[str, Any]:
        """
        Carrega um run completo do Bronze para STG.
        
        Args:
            run_id: ID do run a carregar
            scope: Scope do Zendesk (default: support)
            entities: Lista de entidades a carregar (None = todas)
        
        Returns:
            Relatório de carga
        """
        # Obtém manifest para pegar ingestion_date
        manifest = self.get_manifest(run_id)
        if not manifest:
            raise RuntimeError(f"Manifest não encontrado para run_id={run_id}")
        
        # Extrai ingestion_date do manifest ou calcula do run_id
        ingestion_date = None
        for report in manifest.get("reports", []):
            if report.get("scope") == scope:
                # Tenta extrair do run_id (formato YYYYMMDDTHHMMSSZ)
                try:
                    ingestion_date = f"{run_id[:4]}-{run_id[4:6]}-{run_id[6:8]}"
                except:
                    pass
                break
        
        if not ingestion_date:
            ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        self.logger.info(f"Carregando run {run_id} (ingestion_date={ingestion_date})")
        
        # Define entidades a carregar
        if entities is None:
            entities = list(ENTITY_TABLE_MAP.keys())
        
        report = {
            "run_id": run_id,
            "scope": scope,
            "ingestion_date": ingestion_date,
            "entities": {},
            "total_records": 0,
            "total_parts": 0,
        }
        
        for entity in entities:
            try:
                records, parts = self.load_entity(
                    entity=entity,
                    scope=scope,
                    ingestion_date=ingestion_date,
                    run_id=run_id
                )
                
                report["entities"][entity] = {
                    "records": records,
                    "parts": parts,
                    "status": "SUCCESS"
                }
                report["total_records"] += records
                report["total_parts"] += parts
                
                self.logger.info(f"  {entity}: {records} records ({parts} parts)")
                
            except Exception as e:
                report["entities"][entity] = {
                    "records": 0,
                    "parts": 0,
                    "status": "FAILED",
                    "error": str(e)
                }
                self.logger.error(f"  {entity}: FAILED - {e}")
        
        return report


def main():
    parser = argparse.ArgumentParser(
        description="Carrega dados Zendesk do Bronze (ADLS) para STG (PostgreSQL)"
    )
    parser.add_argument(
        "--run-id",
        help="ID do run a carregar. Se não fornecido, usa o mais recente."
    )
    parser.add_argument(
        "--scope",
        default="support",
        help="Scope do Zendesk (default: support)"
    )
    parser.add_argument(
        "--entities",
        nargs="+",
        help="Entidades a carregar (default: todas)"
    )
    args = parser.parse_args()
    
    # Setup logger
    logger = RunLogger(
        run_name="load_zendesk_stg",
        log_dir=PROJECT_ROOT / "logs"
    )
    
    try:
        # Cria cliente do lake
        lake = LakeClient(
            account=azure_storage.ACCOUNT,
            key=azure_storage.KEY,
            container=azure_storage.CONTAINER
        )
        
        # Cria loader
        loader = ZendeskStgLoader(lake=lake, logger=logger)
        
        # Determina run_id
        run_id = args.run_id
        if not run_id:
            run_id = loader.find_latest_run_id(scope=args.scope)
            if not run_id:
                logger.error("Nenhum run encontrado no Bronze")
                sys.exit(1)
            logger.info(f"Usando run mais recente: {run_id}")
        
        # Executa carga
        report = loader.load_run(
            run_id=run_id,
            scope=args.scope,
            entities=args.entities
        )
        
        # Atualiza métricas
        logger.set_metric("run_id", run_id)
        logger.set_metric("total_records", report["total_records"])
        logger.set_metric("total_parts", report["total_parts"])
        logger.set_metric("entities_loaded", len(report["entities"]))
        
        # Verifica se houve erros
        failed = [e for e, r in report["entities"].items() if r["status"] == "FAILED"]
        if failed:
            logger.error(f"Entidades com falha: {failed}")
        
        # Fecha conexões
        loader.close()
        
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    
    finally:
        summary = logger.log_summary()
        
        # Salva relatório
        report_path = PROJECT_ROOT / "logs" / f"load_zendesk_stg_{run_id}.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
        
        if summary["status"] == "FAILED":
            sys.exit(1)


if __name__ == "__main__":
    main()
