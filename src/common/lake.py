# -*- coding: utf-8 -*-
"""
Utilitários para operações no Azure Data Lake Storage Gen2.
"""
import gzip
import json
from typing import Any, Dict, List, Optional, Iterator
from azure.storage.blob import BlobServiceClient, ContentSettings


class LakeClient:
    """Cliente unificado para operações no ADLS Gen2."""
    
    def __init__(self, account: str, key: str, container: str):
        self.account = account
        self.key = key
        self.container_name = container
        self._service_client = None
        self._container_client = None
    
    @property
    def service_client(self) -> BlobServiceClient:
        if self._service_client is None:
            account_url = f"https://{self.account}.blob.core.windows.net"
            self._service_client = BlobServiceClient(
                account_url=account_url, 
                credential=self.key
            )
        return self._service_client
    
    @property
    def container(self):
        if self._container_client is None:
            self._container_client = self.service_client.get_container_client(self.container_name)
        return self._container_client
    
    def exists(self, path: str) -> bool:
        """Verifica se um blob existe."""
        return self.container.get_blob_client(path).exists()
    
    def read_text(self, path: str) -> Optional[str]:
        """Lê conteúdo de texto de um blob."""
        bc = self.container.get_blob_client(path)
        try:
            return bc.download_blob().readall().decode("utf-8")
        except Exception:
            return None
    
    def read_json(self, path: str) -> Optional[Dict[str, Any]]:
        """Lê e parseia JSON de um blob."""
        text = self.read_text(path)
        if text is None:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None
    
    def read_jsonl_gz(self, path: str) -> List[Dict[str, Any]]:
        """Lê um arquivo .jsonl.gz e retorna lista de dicts."""
        bc = self.container.get_blob_client(path)
        try:
            compressed = bc.download_blob().readall()
            decompressed = gzip.decompress(compressed).decode("utf-8")
            records = []
            for line in decompressed.strip().split("\n"):
                if line.strip():
                    records.append(json.loads(line))
            return records
        except Exception as e:
            raise RuntimeError(f"Erro ao ler {path}: {e}")
    
    def write_bytes(self, path: str, data: bytes, 
                    content_type: str = "application/octet-stream",
                    overwrite: bool = True) -> None:
        """Escreve bytes em um blob."""
        bc = self.container.get_blob_client(path)
        bc.upload_blob(
            data, 
            overwrite=overwrite,
            content_settings=ContentSettings(content_type=content_type)
        )
    
    def write_text(self, path: str, text: str, overwrite: bool = True) -> None:
        """Escreve texto em um blob."""
        self.write_bytes(
            path, 
            text.encode("utf-8"), 
            content_type="text/plain",
            overwrite=overwrite
        )
    
    def write_json(self, path: str, obj: Any, overwrite: bool = True) -> None:
        """Escreve objeto como JSON em um blob."""
        text = json.dumps(obj, ensure_ascii=False, indent=2)
        self.write_bytes(
            path, 
            text.encode("utf-8"), 
            content_type="application/json",
            overwrite=overwrite
        )
    
    def list_blobs(self, prefix: str) -> Iterator[str]:
        """Lista blobs com determinado prefixo."""
        for blob in self.container.list_blobs(name_starts_with=prefix):
            yield blob.name
    
    def list_blobs_details(self, prefix: str) -> Iterator[Dict[str, Any]]:
        """Lista blobs com metadados."""
        for blob in self.container.list_blobs(name_starts_with=prefix):
            yield {
                "name": blob.name,
                "size": blob.size,
                "last_modified": blob.last_modified,
                "content_type": blob.content_settings.content_type if blob.content_settings else None,
            }
    
    def get_latest_run_id(self, source: str, scope: str = None) -> Optional[str]:
        """
        Encontra o run_id mais recente para uma fonte.
        Procura em _meta/{source}/runs/ pelo manifest mais recente.
        """
        prefix = f"_meta/{source}/runs/"
        manifests = []
        
        for blob_name in self.list_blobs(prefix):
            if blob_name.endswith("/manifest.json"):
                # Extrai run_id do path: _meta/{source}/runs/run_id={id}/manifest.json
                parts = blob_name.split("/")
                for part in parts:
                    if part.startswith("run_id="):
                        run_id = part.replace("run_id=", "")
                        manifests.append((run_id, blob_name))
                        break
        
        if not manifests:
            return None
        
        # Ordena por run_id (formato timestamp) e pega o mais recente
        manifests.sort(key=lambda x: x[0], reverse=True)
        return manifests[0][0]
    
    def get_bronze_parts(self, source: str, entity: str, 
                         scope: str = None, run_id: str = None,
                         ingestion_date: str = None) -> List[str]:
        """
        Lista todos os parts de um bronze path.
        Retorna lista de paths completos ordenados.
        """
        # Constrói o prefixo baseado nos parâmetros fornecidos
        if scope:
            prefix = f"bronze/{source}/scope={scope}/entity={entity}/"
        else:
            prefix = f"bronze/{source}/entity={entity}/"
        
        if ingestion_date:
            prefix += f"ingestion_date={ingestion_date}/"
        
        if run_id:
            prefix += f"run_id={run_id}/"
        
        parts = []
        for blob_name in self.list_blobs(prefix):
            if blob_name.endswith(".jsonl.gz"):
                parts.append(blob_name)
        
        # Ordena para garantir ordem consistente
        parts.sort()
        return parts


def create_lake_client() -> LakeClient:
    """Factory para criar cliente do lake com configurações do ambiente."""
    # Import aqui para evitar circular import
    import sys
    sys.path.insert(0, str(__file__).replace("/src/common/lake.py", ""))
    from config.settings import azure_storage
    
    return LakeClient(
        account=azure_storage.ACCOUNT,
        key=azure_storage.KEY,
        container=azure_storage.CONTAINER
    )
