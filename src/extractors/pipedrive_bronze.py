# -*- coding: utf-8 -*-
# MVP: Pipedrive -> ADLS Bronze (jsonl.gz), separado por scope (comercial/expansao)
# - Watermark em ADLS (_meta) por scope+entity
# - Deals sem excluídos (status=open,won,lost)

import os, json, gzip, uuid, argparse, time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Iterator, List, Optional, Tuple

import requests
from dateutil import parser as dtp
from azure.storage.blob import BlobServiceClient

from dotenv import load_dotenv
from pathlib import Path

# ======================
# Load config/.env (Windows-friendly)
# ======================
def _load_project_env() -> None:
    # resolve raiz do projeto mesmo se executar de qualquer pasta
    this_file = Path(__file__).resolve()
    project_root = this_file.parents[2]  # .../src/extractors/pipedrive_bronze.py -> raiz do repositório 
    env_path = project_root / "config" / ".env"
    load_dotenv(env_path, override=True)

_load_project_env()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_rfc3339(dt: datetime) -> str:
    s = dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    return s.replace("+00:00", "Z")


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


class LakeWriter:
    def __init__(self, account: str, key: str, container: str):
        svc = BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net", credential=key)
        self.container = svc.get_container_client(container)

    def exists(self, path: str) -> bool:
        return self.container.get_blob_client(path).exists()

    def read_text(self, path: str) -> str:
        bc = self.container.get_blob_client(path)
        return bc.download_blob().readall().decode("utf-8")

    def write_bytes(self, path: str, data: bytes, overwrite: bool = True) -> None:
        self.container.get_blob_client(path).upload_blob(data, overwrite=overwrite)

    def write_text(self, path: str, text: str, overwrite: bool = True) -> None:
        self.write_bytes(path, text.encode("utf-8"), overwrite=overwrite)


class PipedriveClient:
    def __init__(self, company_domain: str, api_token: str):
        self.base_v2 = f"https://{company_domain}.pipedrive.com/api/v2"
        self.base_v1 = f"https://{company_domain}.pipedrive.com/api/v1"
        self.session = requests.Session()
        self.session.headers.update({"x-api-token": api_token})
        self.timeout = 60

    def _request(self, url: str, params: Dict[str, Any], max_retries: int = 8) -> requests.Response:
        # backoff simples para 429/5xx
        for attempt in range(max_retries):
            r = self.session.get(url, params=params, timeout=self.timeout)

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                sleep_s = float(ra) if ra and ra.isdigit() else min(30.0, 2 ** attempt)
                time.sleep(sleep_s)
                continue

            if 500 <= r.status_code < 600:
                time.sleep(min(30.0, 2 ** attempt))
                continue

            return r

        return r

    def get_json(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        r = self._request(url, params)
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code} - {r.text[:500]}")
        return r.json()

    def iter_cursor_pages(self, path: str, params: Dict[str, Any]) -> Iterator[Tuple[List[Dict[str, Any]], Optional[str]]]:
        url = f"{self.base_v2}{path}"
        cursor = None
        while True:
            q = dict(params)
            q["limit"] = q.get("limit", 500)
            if cursor:
                q["cursor"] = cursor

            payload = self.get_json(url, q)
            data = payload.get("data") or []
            add = payload.get("additional_data") or {}
            next_cursor = add.get("next_cursor", None)

            yield data, next_cursor

            if not next_cursor:
                break
            cursor = next_cursor

    def get_users_v1(self) -> List[Dict[str, Any]]:
        url = f"{self.base_v1}/users"
        payload = self.get_json(url, params={})
        return payload.get("data") or []


def wm_path(scope: str, entity: str) -> str:
    return f"_meta/pipedrive/watermarks/scope={scope}/entity={entity}.json"


def load_watermark(lake: LakeWriter, scope: str, entity: str) -> Optional[datetime]:
    p = wm_path(scope, entity)
    if not lake.exists(p):
        return None
    obj = json.loads(lake.read_text(p))
    ts = obj.get("last_successful_until")
    return dtp.isoparse(ts) if ts else None


def save_watermark(lake: LakeWriter, scope: str, entity: str, last_successful_until: datetime) -> None:
    p = wm_path(scope, entity)
    payload = {
        "scope": scope,
        "entity": entity,
        "last_successful_until": to_rfc3339(last_successful_until),
        "updated_at_utc": to_rfc3339(utc_now())
    }
    lake.write_text(p, json_dumps(payload), overwrite=True)


def bronze_prefix(scope: str, entity: str, ingestion_date: str, run_id: str) -> str:
    return f"bronze/pipedrive/scope={scope}/entity={entity}/ingestion_date={ingestion_date}/run_id={run_id}"


def write_jsonl_gz(lake: LakeWriter, path: str, rows: List[Dict[str, Any]]) -> int:
    buf = "\n".join(json_dumps(r) for r in rows).encode("utf-8")
    gz = gzip.compress(buf, compresslevel=6)
    lake.write_bytes(path, gz, overwrite=True)
    return len(rows)


def build_entity_params(entity: str, since_dt: Optional[datetime], until_dt: datetime) -> Dict[str, Any]:
    params: Dict[str, Any] = {}

    if entity == "deals":
        # não capturar excluídos
        params["status"] = "open,won,lost"
        params["sort_by"] = "update_time"
        params["sort_direction"] = "asc"
        if since_dt:
            params["updated_since"] = to_rfc3339(since_dt)
        params["updated_until"] = to_rfc3339(until_dt)

    elif entity in ("persons", "organizations"):
        params["sort_by"] = "update_time"
        params["sort_direction"] = "asc"
        if since_dt:
            params["updated_since"] = to_rfc3339(since_dt)
        params["updated_until"] = to_rfc3339(until_dt)

    elif entity == "activities":
        # v2 activities: usar updated_since/updated_until (nao since/until)
        if since_dt:
            params["updated_since"] = to_rfc3339(since_dt)
        params["updated_until"] = to_rfc3339(until_dt)
        params["sort_by"] = "update_time"
        params["sort_direction"] = "asc"
    return params


def entity_path(entity: str) -> str:
    mapping = {
        "deals": "/deals",
        "persons": "/persons",
        "organizations": "/organizations",
        "activities": "/activities",
        "pipelines": "/pipelines",
        "stages": "/stages",
    }
    return mapping[entity]


def run_scope(
    lake: LakeWriter,
    company_domain: str,
    scope: str,
    token: str,
    entities: List[str],
    overlap_minutes: int = 5,
    batch_pages: int = 5,
    max_buffer_records: int = 10000
) -> Dict[str, Any]:
    """
    Batching para reduzir small files:
    - acumula 'batch_pages' páginas (ou até max_buffer_records registros) e grava 1 part.
    """
    client = PipedriveClient(company_domain, token)

    run_id = to_rfc3339(utc_now()).replace(":", "").replace("-", "")
    ingestion_date = utc_now().date().isoformat()
    until_dt = utc_now()

    scope_report: Dict[str, Any] = {"scope": scope, "entities": {}, "run_id": run_id}

    for entity in entities:
        # users = snapshot v1
        if entity == "users":
            users = client.get_users_v1()
            base = bronze_prefix(scope, entity, ingestion_date, run_id)
            out = f"{base}/part-00001.jsonl.gz"
            n = write_jsonl_gz(lake, out, users)
            scope_report["entities"][entity] = {"mode": "snapshot", "records": n, "parts": 1}
            save_watermark(lake, scope, entity, until_dt)
            continue

        is_snapshot = entity in ("pipelines", "stages")
        wm = load_watermark(lake, scope, entity)

        since_dt = None
        mode = "snapshot"
        if not is_snapshot:
            mode = "incremental"
            if wm:
                since_dt = wm - timedelta(minutes=overlap_minutes)

        params = build_entity_params(entity, since_dt, until_dt)

        base = bronze_prefix(scope, entity, ingestion_date, run_id)
        total = 0
        parts = 0

        buf: List[Dict[str, Any]] = []
        pages_in_buf = 0

        for _page_i, (rows, _next_cursor) in enumerate(client.iter_cursor_pages(entity_path(entity), params), start=1):
            # filtro extra de deleção, caso apareça
            filtered = []
            for r in rows:
                if isinstance(r, dict) and (r.get("deleted") is True or r.get("is_deleted") is True):
                    continue
                filtered.append(r)

            buf.extend(filtered)
            pages_in_buf += 1

            if pages_in_buf >= batch_pages or len(buf) >= max_buffer_records:
                parts += 1
                out = f"{base}/part-{parts:05d}.jsonl.gz"
                total += write_jsonl_gz(lake, out, buf)
                buf = []
                pages_in_buf = 0

        # flush final
        if buf:
            parts += 1
            out = f"{base}/part-{parts:05d}.jsonl.gz"
            total += write_jsonl_gz(lake, out, buf)

        scope_report["entities"][entity] = {"mode": mode, "records": total, "parts": parts}
        save_watermark(lake, scope, entity, until_dt)

    return scope_report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scopes", nargs="+", default=["comercial", "expansao"])
    ap.add_argument("--entities", nargs="+",
                    default=["deals", "persons", "organizations", "activities", "pipelines", "stages", "users"])
    ap.add_argument("--overlap-minutes", type=int, default=5)
    args = ap.parse_args()

    company_domain = os.environ["PIPEDRIVE_COMPANY_DOMAIN"]
    container = os.environ.get("ADLS_CONTAINER", "datalake")
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    key = os.environ["AZURE_STORAGE_KEY"]

    token_map = {
        "comercial": os.environ.get("PIPEDRIVE_TOKEN_COMERCIAL"),
        "expansao": os.environ.get("PIPEDRIVE_TOKEN_EXPANSAO"),
    }

    lake = LakeWriter(account, key, container)

    run_manifest = {
        "run_started_at_utc": to_rfc3339(utc_now()),
        "company_domain": company_domain,
        "container": container,
        "scopes": args.scopes,
        "entities": args.entities,
        "reports": []
    }

    for scope in args.scopes:
        tok = token_map.get(scope)
        if not tok:
            raise SystemExit(f"Missing token for scope '{scope}'")
        rep = run_scope(lake, company_domain, scope, tok, args.entities, overlap_minutes=args.overlap_minutes)
        run_manifest["reports"].append(rep)

    run_manifest["run_finished_at_utc"] = to_rfc3339(utc_now())

    rid = run_manifest["reports"][0]["run_id"] if run_manifest["reports"] else str(uuid.uuid4())
    lake.write_text(f"_meta/pipedrive/runs/run_id={rid}/manifest.json", json_dumps(run_manifest), overwrite=True)

    print(json.dumps(run_manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
