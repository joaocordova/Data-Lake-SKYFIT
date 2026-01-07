# -*- coding: utf-8 -*-
"""
Zendesk -> Bronze (ADLS Gen2 via Blob SDK)
- tickets: incremental export cursor-based (exclude_deleted=true)
- users, organizations: incremental export time-based (start_time)
- groups, ticket_fields, ticket_forms: snapshot

Outputs:
  bronze/zendesk/scope=<scope>/entity=<entity>/ingestion_date=YYYY-MM-DD/run_id=<run_id>/part-xxxxx.jsonl.gz
  _meta/zendesk/watermarks/scope=<scope>/entity=<entity>.json
  _meta/zendesk/runs/run_id=<run_id>/manifest.json
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import requests
from requests.auth import HTTPBasicAuth
from azure.storage.blob import BlobServiceClient, ContentSettings

from dotenv import load_dotenv
load_dotenv("config/.env", override=True)


# --- LOAD .ENV (config/.env) ---
def load_env_file() -> None:
    """
    Carrega variáveis do arquivo config/.env para os.environ
    (funciona com python-dotenv; se não tiver, faz fallback simples).
    """
    proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    env_path = os.path.join(proj_root, "config", ".env")

    # tenta python-dotenv
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(env_path, override=False)
        return
    except Exception:
        pass

    # fallback simples (KEY=VALUE)
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k.strip(), v)

load_env_file()
# --- END LOAD .ENV ---





def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def run_id_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


@dataclass
class Lake:
    account: str
    key: str
    container: str

    @property
    def account_url(self) -> str:
        return f"https://{self.account}.blob.core.windows.net"

    def client(self) -> BlobServiceClient:
        return BlobServiceClient(account_url=self.account_url, credential=self.key)

    def upload_bytes(self, path: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        bsc = self.client()
        bc = bsc.get_blob_client(container=self.container, blob=path)
        bc.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type=content_type))

    def download_text(self, path: str) -> Optional[str]:
        bsc = self.client()
        bc = bsc.get_blob_client(container=self.container, blob=path)
        try:
            return bc.download_blob().readall().decode("utf-8")
        except Exception:
            return None


class ZendeskClient:
    def __init__(self, subdomain: str, email: str, api_token: str, timeout: int = 60):
        self.base = f"https://{subdomain}.zendesk.com"
        self.sess = requests.Session()
        self.auth = HTTPBasicAuth(f"{email}/token", api_token)
        self.timeout = timeout
        self.sess.headers.update({"Content-Type": "application/json"})

    def _request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(1, 9):
            r = self.sess.get(url, params=params, auth=self.auth, timeout=self.timeout)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                sleep_s = int(ra) if (ra and ra.isdigit()) else min(60, 2 ** attempt)
                time.sleep(sleep_s)
                continue
            if r.status_code >= 500:
                time.sleep(min(60, 2 ** attempt))
                continue
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code} - {r.text[:1000]}")
            return r.json()
        raise RuntimeError("Too many retries talking to Zendesk API")

    def snapshot_paginated(self, path: str, root_key: str, per_page: int = 100) -> Iterable[List[Dict[str, Any]]]:
        url = f"{self.base}{path}"
        params = {"per_page": per_page}
        while True:
            payload = self._request(url, params=params)
            rows = payload.get(root_key, []) or []
            yield rows
            next_page = payload.get("next_page")
            if not next_page:
                break
            url = next_page
            params = {}

    def incremental_tickets_cursor(
        self,
        start_time: int,
        cursor: Optional[str],
        per_page: int,
        exclude_deleted: bool,
    ) -> Iterable[Tuple[List[Dict[str, Any]], Optional[str], bool]]:
        """
        Cursor-based tickets export.
        Response SHOULD include after_cursor and end_of_stream. Porém, para robustez:
        - tenta localizar after_cursor em payload/meta/links/next_page
        - se end_of_stream=True e cursor não veio, permite encerrar sem atualizar watermark
        """
        url = f"{self.base}/api/v2/incremental/tickets/cursor.json"
        params: Dict[str, Any] = {"per_page": per_page}
        if cursor:
            params["cursor"] = cursor
        else:
            params["start_time"] = start_time
        if exclude_deleted:
            params["exclude_deleted"] = "true"

        while True:
            payload = self._request(url, params=params)
            rows = payload.get("tickets", []) or []
            eos = bool(payload.get("end_of_stream"))

            after_cursor = payload.get("after_cursor")
            if not after_cursor:
                after_cursor = (payload.get("meta") or {}).get("after_cursor")
            if not after_cursor:
                next_page = payload.get("next_page") or (payload.get("links") or {}).get("next")
                if next_page:
                    try:
                        qs = parse_qs(urlparse(next_page).query)
                        after_cursor = (qs.get("cursor") or qs.get("after_cursor") or [None])[0]
                    except Exception:
                        after_cursor = None

            yield rows, (str(after_cursor) if after_cursor else None), eos

            if eos:
                break

            if not after_cursor:
                # não dá para paginar; falha explícita com contexto
                keys = sorted(list(payload.keys()))
                raise RuntimeError(f"Missing after_cursor (and not end_of_stream). payload_keys={keys}")

            params = {"cursor": str(after_cursor), "per_page": per_page}
            if exclude_deleted:
                params["exclude_deleted"] = "true"


def gz_jsonl(records: List[Dict[str, Any]]) -> bytes:
    bio = io.BytesIO()
    with gzip.GzipFile(fileobj=bio, mode="wb") as gz:
        for r in records:
            gz.write((json.dumps(r, ensure_ascii=False) + "\n").encode("utf-8"))
    return bio.getvalue()


def wm_path(scope: str, entity: str) -> str:
    return f"_meta/zendesk/watermarks/scope={scope}/entity={entity}.json"


def runs_manifest_path(rid: str) -> str:
    return f"_meta/zendesk/runs/run_id={rid}/manifest.json"


def bronze_part_path(scope: str, entity: str, ingestion_date: str, rid: str, part: int) -> str:
    return (
        f"bronze/zendesk/scope={scope}/entity={entity}/"
        f"ingestion_date={ingestion_date}/run_id={rid}/part-{part:05d}.jsonl.gz"
    )


def load_watermark(lake: Lake, scope: str, entity: str) -> Dict[str, Any]:
    txt = lake.download_text(wm_path(scope, entity))
    if not txt:
        return {}
    try:
        return json.loads(txt)
    except Exception:
        return {}


def save_watermark(lake: Lake, scope: str, entity: str, obj: Dict[str, Any]) -> None:
    obj = dict(obj)
    obj["updated_at_utc"] = utc_now_iso()
    lake.upload_bytes(wm_path(scope, entity), json.dumps(obj, ensure_ascii=False).encode("utf-8"), content_type="application/json")


def write_batches(
    lake: Lake,
    scope: str,
    entity: str,
    rid: str,
    ingestion_date: str,
    batch_pages: int,
    page_iter: Iterable[List[Dict[str, Any]]],
) -> Tuple[int, int]:
    total = 0
    parts = 0
    batch: List[Dict[str, Any]] = []
    pages_in_batch = 0

    def flush():
        nonlocal parts, batch, pages_in_batch
        if not batch:
            return
        parts += 1
        path = bronze_part_path(scope, entity, ingestion_date, rid, parts)
        lake.upload_bytes(path, gz_jsonl(batch), content_type="application/gzip")
        batch = []
        pages_in_batch = 0

    for rows in page_iter:
        if rows:
            total += len(rows)
            batch.extend(rows)
        pages_in_batch += 1
        if pages_in_batch >= batch_pages:
            flush()
    flush()
    return total, parts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scopes", nargs="+", default=["support"])
    ap.add_argument("--entities", nargs="+", default=["tickets", "users", "organizations", "groups", "ticket_fields", "ticket_forms"])
    ap.add_argument("--start-time", type=int, default=0, help="Unix epoch para carga inicial (time-based/cursor init).")
    ap.add_argument("--per-page", type=int, default=1000)
    ap.add_argument("--batch-pages", type=int, default=8)
    ap.add_argument("--exclude-deleted", action="store_true", default=True)
    args = ap.parse_args()

    lake = Lake(
        account=ensure_env("AZURE_STORAGE_ACCOUNT"),
        key=ensure_env("AZURE_STORAGE_KEY"),
        container=ensure_env("ADLS_CONTAINER"),
    )

    subdomain = ensure_env("ZENDESK_SUBDOMAIN")
    email = ensure_env("ZENDESK_EMAIL")
    token = ensure_env("ZENDESK_API_TOKEN")
    zd = ZendeskClient(subdomain=subdomain, email=email, api_token=token)

    rid = run_id_utc()
    ingestion_date = utc_today()
    report: Dict[str, Any] = {
        "run_started_at_utc": utc_now_iso(),
        "zendesk_subdomain": subdomain,
        "container": lake.container,
        "scopes": args.scopes,
        "entities": args.entities,
        "reports": [],
    }

    snapshots = {
        "groups": ("/api/v2/groups.json", "groups"),
        "ticket_fields": ("/api/v2/ticket_fields.json", "ticket_fields"),
        "ticket_forms": ("/api/v2/ticket_forms.json", "ticket_forms"),
    }
    time_based = {
        "users": ("/api/v2/incremental/users.json", "users"),
        "organizations": ("/api/v2/incremental/organizations.json", "organizations"),
    }

    for scope in args.scopes:
        scope_rep: Dict[str, Any] = {"scope": scope, "entities": {}, "run_id": rid}

        for entity in args.entities:
            if entity == "tickets":
                wm = load_watermark(lake, scope, "tickets")
                cursor = wm.get("cursor")
                start_time = int(wm.get("start_time") or args.start_time)

                last_after: Optional[str] = None
                last_eos = False

                def pages():
                    nonlocal last_after, last_eos
                    for rows, after_cursor, eos in zd.incremental_tickets_cursor(
                        start_time=start_time,
                        cursor=cursor,
                        per_page=args.per_page,
                        exclude_deleted=args.exclude_deleted,
                    ):
                        last_eos = eos
                        if after_cursor:
                            last_after = after_cursor
                        yield rows

                total, parts = write_batches(
                    lake=lake, scope=scope, entity="tickets",
                    rid=rid, ingestion_date=ingestion_date,
                    batch_pages=args.batch_pages, page_iter=pages()
                )

                if last_after:
                    save_watermark(lake, scope, "tickets", {"mode": "cursor", "cursor": last_after, "start_time": start_time, "end_of_stream": last_eos})

                scope_rep["entities"]["tickets"] = {"mode": "incremental_cursor", "records": total, "parts": parts}

            elif entity in time_based:
                path, root = time_based[entity]
                wm = load_watermark(lake, scope, entity)
                start_time = int(wm.get("start_time") or args.start_time)
                end_time_holder = {"end_time": start_time}

                def pages():
                    url = f"https://{subdomain}.zendesk.com{path}"
                    params = {"start_time": start_time, "per_page": args.per_page}
                    while True:
                        payload = zd._request(url, params=params)
                        rows = payload.get(root, []) or []
                        yield rows
                        end_time_holder["end_time"] = int(payload.get("end_time") or end_time_holder["end_time"])
                        if payload.get("end_of_stream") is True:
                            break
                        next_page = payload.get("next_page")
                        if not next_page:
                            break
                        url = next_page
                        params = {}

                total, parts = write_batches(
                    lake=lake, scope=scope, entity=entity,
                    rid=rid, ingestion_date=ingestion_date,
                    batch_pages=args.batch_pages, page_iter=pages()
                )
                save_watermark(lake, scope, entity, {"mode": "time", "start_time": end_time_holder["end_time"]})
                scope_rep["entities"][entity] = {"mode": "incremental_time", "records": total, "parts": parts, "next_start_time": end_time_holder["end_time"]}

            elif entity in snapshots:
                path, root = snapshots[entity]

                def pages():
                    yield from zd.snapshot_paginated(path=path, root_key=root, per_page=100)

                total, parts = write_batches(
                    lake=lake, scope=scope, entity=entity,
                    rid=rid, ingestion_date=ingestion_date,
                    batch_pages=max(1, args.batch_pages), page_iter=pages()
                )
                scope_rep["entities"][entity] = {"mode": "snapshot", "records": total, "parts": parts}

            else:
                raise RuntimeError(f"Unknown entity: {entity}")

        report["reports"].append(scope_rep)

    report["run_finished_at_utc"] = utc_now_iso()
    lake.upload_bytes(runs_manifest_path(rid), json.dumps(report, ensure_ascii=False).encode("utf-8"), content_type="application/json")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
