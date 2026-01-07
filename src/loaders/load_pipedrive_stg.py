# -*- coding: utf-8 -*-
"""
Loader: Pipedrive Bronze (ADLS) -> STG (PostgreSQL)

Carrega todos os arquivos .jsonl.gz de um run do Bronze para as tabelas
stg_pipedrive.*_raw no PostgreSQL com rastreabilidade completa.

Nota: Pipedrive tem múltiplos scopes (comercial, expansao), cada um com seu token.

Uso:
    python load_pipedrive_stg.py [--run-id <ID>] [--scopes <list>] [--entities <list>]
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
    "deals": "deals_raw",
    "persons": "persons_raw",
    "organizations": "organizations_raw",
    "activities": "activities_raw",
    "pipelines": "pipelines_raw",
    "stages": "stages_raw",
    "users": "users_raw",
}

# Schema do STG
STG_SCHEMA = "stg_pipedrive"

# Scopes disponíveis
DEFAULT_SCOPES = ["comercial", "expansao"]


class PipedriveStgLoader:
    """Carrega dados do Bronze Pipedrive para STG no PostgreSQL."""
    
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
    
    def find_latest_run_id(self) -> Optional[str]:
        """Encontra o run_id mais recente para Pipedrive."""
        prefix = "_meta/pipedrive/runs/"
        manifests = []
        
        for blob_name in self.lake.list_blobs(prefix):
            if blob_name.endswith("/manifest.json"):
                parts = blob_name.split("/")
                for part in parts:
                    if part.startswith("run_id="):
                        run_id = part.replace("run_id=", "")
                        manifests.append((run_id, blob_name))
                        break
        
        if not manifests:
            return None
        
        # Ordena por run_id (formato timestamp)
        manifests.sort(key=lambda x: x[0], reverse=True)
        return manifests[0][0]
    
    def get_manifest(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Obtém o manifest de um run."""
        path = f"_meta/pipedrive/runs/run_id={run_id}/manifest.json"
        return self.lake.read_json(path)
    
    def find_latest_run_id_for_scope(self, scope: str, ingestion_date: str) -> Optional[str]:
        """
        Encontra o run_id mais recente para um scope específico.
        
        IMPORTANTE: O extrator gera run_ids separados por scope (com alguns segundos
        de diferença), então precisamos buscar o run_id diretamente no Bronze,
        não no manifest global.
        """
        # Busca diretamente no Bronze - usa 'deals' como entidade de referência
        prefix = f"bronze/pipedrive/scope={scope}/entity=deals/ingestion_date={ingestion_date}/"
        
        run_ids = set()
        for blob_name in self.lake.list_blobs(prefix):
            parts = blob_name.split("/")
            for part in parts:
                if part.startswith("run_id="):
                    run_ids.add(part.replace("run_id=", ""))
                    break
        
        if not run_ids:
            # Tenta sem filtro de data (busca qualquer data)
            prefix_any = f"bronze/pipedrive/scope={scope}/entity=deals/"
            for blob_name in self.lake.list_blobs(prefix_any):
                parts = blob_name.split("/")
                for part in parts:
                    if part.startswith("run_id="):
                        run_ids.add(part.replace("run_id=", ""))
                        break
        
        if not run_ids:
            return None
        
        # Retorna o mais recente (formato timestamp, ordenação alfabética funciona)
        return sorted(run_ids, reverse=True)[0]
    
    def list_bronze_parts(self, entity: str, scope: str, 
                          ingestion_date: str, run_id: str) -> List[str]:
        """Lista todos os parts de uma entidade no Bronze."""
        prefix = (
            f"bronze/pipedrive/scope={scope}/entity={entity}/"
            f"ingestion_date={ingestion_date}/run_id={run_id}/"
        )
        
        parts = []
        for blob_name in self.lake.list_blobs(prefix):
            if blob_name.endswith(".jsonl.gz"):
                parts.append(blob_name)
        
        parts.sort()
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
            self.logger.debug(f"Nenhum part encontrado para {scope}/{entity}")
            return 0, 0
        
        self.logger.info(f"Carregando {scope}/{entity}: {len(parts)} parts")
        
        # SQL de inserção com ON CONFLICT para idempotência
        # Pipedrive tem scope como parte da chave de dedupe
        insert_sql = f"""
            INSERT INTO {STG_SCHEMA}.{table_name} 
                (payload, scope, source_blob_path, source_line_no, run_id, ingestion_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (scope, source_blob_path, source_line_no) 
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
                        scope,
                        part_path,
                        line_no,
                        run_id,
                        ingestion_date
                    ))
                    
                    if len(batch) >= batch_size:
                        cursor.executemany(insert_sql, batch)
                        total_records += len(batch)
                        batch = []
                
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
    
    def load_run(self, run_id: str, 
                 scopes: List[str] = None,
                 entities: List[str] = None) -> Dict[str, Any]:
        """
        Carrega um run completo do Bronze para STG.
        
        Args:
            run_id: ID do run base (usado para ingestion_date)
            scopes: Lista de scopes a carregar (None = todos)
            entities: Lista de entidades a carregar (None = todas)
        
        Returns:
            Relatório de carga
        """
        # Extrai ingestion_date do run_id (formato: 20250106T020000Z)
        try:
            ingestion_date = f"{run_id[:4]}-{run_id[4:6]}-{run_id[6:8]}"
        except:
            ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        self.logger.info(f"Carregando dados de {ingestion_date}")
        
        # Define scopes e entidades
        if scopes is None:
            scopes = DEFAULT_SCOPES
        if entities is None:
            entities = list(ENTITY_TABLE_MAP.keys())
        
        report = {
            "run_id": run_id,
            "ingestion_date": ingestion_date,
            "scopes": {},
            "total_records": 0,
            "total_parts": 0,
        }
        
        for scope in scopes:
            # IMPORTANTE: Cada scope tem seu próprio run_id no Bronze
            scope_run_id = self.find_latest_run_id_for_scope(scope, ingestion_date)
            
            if not scope_run_id:
                self.logger.warning(f"Nenhum run encontrado para scope={scope} em {ingestion_date}")
                report["scopes"][scope] = {"entities": {}, "records": 0, "parts": 0, "run_id": None}
                continue
            
            self.logger.info(f"Scope {scope}: usando run_id={scope_run_id}")
            
            scope_report = {"entities": {}, "records": 0, "parts": 0, "run_id": scope_run_id}
            
            for entity in entities:
                try:
                    records, parts = self.load_entity(
                        entity=entity,
                        scope=scope,
                        ingestion_date=ingestion_date,
                        run_id=scope_run_id  # USA O RUN_ID DO SCOPE!
                    )
                    
                    scope_report["entities"][entity] = {
                        "records": records,
                        "parts": parts,
                        "status": "SUCCESS"
                    }
                    scope_report["records"] += records
                    scope_report["parts"] += parts
                    
                    if records > 0:
                        self.logger.info(f"  {scope}/{entity}: {records} records ({parts} parts)")
                    
                except Exception as e:
                    scope_report["entities"][entity] = {
                        "records": 0,
                        "parts": 0,
                        "status": "FAILED",
                        "error": str(e)
                    }
                    self.logger.error(f"  {scope}/{entity}: FAILED - {e}")
            
            report["scopes"][scope] = scope_report
            report["total_records"] += scope_report["records"]
            report["total_parts"] += scope_report["parts"]
        
        return report


def main():
    parser = argparse.ArgumentParser(
        description="Carrega dados Pipedrive do Bronze (ADLS) para STG (PostgreSQL)"
    )
    parser.add_argument(
        "--run-id",
        help="ID do run a carregar. Se não fornecido, usa o mais recente."
    )
    parser.add_argument(
        "--scopes",
        nargs="+",
        default=DEFAULT_SCOPES,
        help=f"Scopes a carregar (default: {DEFAULT_SCOPES})"
    )
    parser.add_argument(
        "--entities",
        nargs="+",
        help="Entidades a carregar (default: todas)"
    )
    args = parser.parse_args()
    
    # Setup logger
    logger = RunLogger(
        run_name="load_pipedrive_stg",
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
        loader = PipedriveStgLoader(lake=lake, logger=logger)
        
        # Determina run_id
        run_id = args.run_id
        if not run_id:
            run_id = loader.find_latest_run_id()
            if not run_id:
                logger.error("Nenhum run encontrado no Bronze")
                sys.exit(1)
            logger.info(f"Usando run mais recente: {run_id}")
        
        # Executa carga
        report = loader.load_run(
            run_id=run_id,
            scopes=args.scopes,
            entities=args.entities
        )
        
        # Atualiza métricas
        logger.set_metric("run_id", run_id)
        logger.set_metric("total_records", report["total_records"])
        logger.set_metric("total_parts", report["total_parts"])
        logger.set_metric("scopes_loaded", len(report["scopes"]))
        
        # Verifica erros
        for scope, scope_data in report["scopes"].items():
            failed = [e for e, r in scope_data["entities"].items() if r["status"] == "FAILED"]
            if failed:
                logger.error(f"Entidades com falha em {scope}: {failed}")
        
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
        report_path = PROJECT_ROOT / "logs" / f"load_pipedrive_stg_{run_id}.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
        
        if summary["status"] == "FAILED":
            sys.exit(1)


if __name__ == "__main__":
    main()
