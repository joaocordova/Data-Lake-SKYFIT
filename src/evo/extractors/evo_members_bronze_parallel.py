# -*- coding: utf-8 -*-
"""
Extractor PARALELO: EVO Members → Bronze (CORRIGIDO v2)

Extrai membros com seus contratos (memberships).
Otimizado para alto volume com multiprocessing.

IMPORTANTE: showMemberships=True para trazer contratos

Uso:
    cd C:/skyfit-datalake/evo_members
    python src/extractors/evo_members_bronze_parallel.py --start-date 2020-06-01 --end-date 2026-01-09 --workers 4
"""
import argparse
import gzip
import json
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# Silencia logs verbosos
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)


def load_env():
    """Carrega .env usando python-dotenv."""
    env_paths = [
        PROJECT_ROOT / "config" / ".env",
        PROJECT_ROOT / ".env",
        PROJECT_ROOT.parent / "config" / ".env",
        Path(r"C:\skyfit-datalake\config\.env"),
    ]
    try:
        from dotenv import load_dotenv
        for p in env_paths:
            if p.exists():
                load_dotenv(p, override=True)
                print(f"[ENV] Carregado: {p}")
                return True
    except ImportError:
        print("[ENV] ERRO: python-dotenv não instalado")
        print("[ENV] Execute: pip install python-dotenv")
        sys.exit(1)
    return False


load_env()

# Configuração - ACEITA AMBOS OS NOMES DE VARIÁVEIS
ENV = {
    "EVO_API_URL": os.environ.get("EVO_API_URL", "https://evo-integracao.w12app.com.br"),
    "EVO_USERNAME": os.environ.get("EVO_USERNAME"),
    "EVO_PASSWORD": os.environ.get("EVO_PASSWORD"),
    # Azure - aceita tanto os nomes antigos quanto os novos
    "AZURE_STORAGE_ACCOUNT_NAME": os.environ.get("AZURE_STORAGE_ACCOUNT") or os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
    "AZURE_STORAGE_ACCOUNT_KEY": os.environ.get("AZURE_STORAGE_KEY") or os.environ.get("AZURE_STORAGE_ACCOUNT_KEY"),
    "AZURE_CONTAINER_NAME": os.environ.get("ADLS_CONTAINER") or os.environ.get("AZURE_CONTAINER_NAME", "datalake"),
}

# Validação
def validate_env():
    errors = []
    if not ENV.get("EVO_USERNAME"):
        errors.append("EVO_USERNAME não definida")
    if not ENV.get("EVO_PASSWORD"):
        errors.append("EVO_PASSWORD não definida")
    if not ENV.get("AZURE_STORAGE_ACCOUNT_NAME"):
        errors.append("AZURE_STORAGE_ACCOUNT não definida")
    if not ENV.get("AZURE_STORAGE_ACCOUNT_KEY"):
        errors.append("AZURE_STORAGE_KEY não definida")
    
    key_len = len(ENV.get("AZURE_STORAGE_ACCOUNT_KEY", ""))
    if key_len < 50:
        errors.append(f"AZURE_STORAGE_KEY parece truncada (len={key_len}, esperado ~88)")
    
    if errors:
        print("[ENV] ERROS:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    
    print(f"[ENV] Azure Account: {ENV['AZURE_STORAGE_ACCOUNT_NAME']}")
    print(f"[ENV] Azure Container: {ENV['AZURE_CONTAINER_NAME']}")
    print(f"[ENV] Azure Key length: {key_len}")
    print(f"[ENV] EVO User: {ENV['EVO_USERNAME']}")


class RateLimiter:
    """Rate limiter - janela livre 0h-5h."""
    def __init__(self, rpm: int = 40):
        self.min_interval = 60.0 / rpm
        self.last_request = 0
        self.count = 0
    
    def wait(self):
        hour = datetime.now().hour
        if 0 <= hour < 5:
            time.sleep(0.02)
            self.count += 1
            return
        
        elapsed = time.time() - self.last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request = time.time()
        self.count += 1


def extract_period_worker(args: Tuple) -> Dict:
    """
    Worker que extrai um período específico.
    Usa registerDateStart/registerDateEnd para particionar.
    """
    start_date, end_date, run_id, worker_id, env_copy = args
    
    # Usa env_copy passado como argumento (evita problemas de pickle)
    from azure.storage.filedatalake import DataLakeServiceClient
    
    rate_limiter = RateLimiter(rpm=40)
    session = requests.Session()
    session.auth = (env_copy["EVO_USERNAME"], env_copy["EVO_PASSWORD"])
    
    # Azure client
    try:
        service = DataLakeServiceClient(
            account_url=f"https://{env_copy['AZURE_STORAGE_ACCOUNT_NAME']}.dfs.core.windows.net",
            credential=env_copy["AZURE_STORAGE_ACCOUNT_KEY"]
        )
        fs = service.get_file_system_client(env_copy["AZURE_CONTAINER_NAME"])
    except Exception as e:
        return {"error": f"Azure connection failed: {e}", "period": f"{start_date} - {end_date}"}
    
    base_path = f"bronze/evo/entity=members/ingestion_date={datetime.now(timezone.utc).strftime('%Y-%m-%d')}/run_id={run_id}"
    
    skip = 0
    take = 50  # Máximo permitido pela API
    buffer = []
    part_num = 0
    total = 0
    batch_size = 2000
    prefix = f"{start_date[:7]}_"
    
    start_time = time.time()
    retry_count = 0
    max_retries = 5
    
    while True:
        try:
            rate_limiter.wait()
            
            url = f"{env_copy['EVO_API_URL']}/api/v2/members"
            params = {
                "skip": skip,
                "take": take,
                "showMemberships": "true",
                "showsResponsibles": "true",
                "registerDateStart": start_date,
                "registerDateEnd": end_date,
            }
            
            resp = session.get(url, params=params, timeout=120)
            
            if resp.status_code == 401:
                return {"error": "401 Unauthorized - verifique EVO_USERNAME/EVO_PASSWORD", "period": f"{start_date} - {end_date}"}
            
            if resp.status_code == 500:
                retry_count += 1
                if retry_count > max_retries:
                    print(f"[Worker {worker_id}] ⚠️ 500 persistente em {start_date} skip={skip} - pulando")
                    skip += take
                    retry_count = 0
                    continue
                print(f"[Worker {worker_id}] ⚠️ 500 em {start_date} skip={skip} - retry {retry_count}/{max_retries}")
                time.sleep(5)
                continue
            
            resp.raise_for_status()
            data = resp.json()
            retry_count = 0
            
            records = data if isinstance(data, list) else data.get("data", [])
            
            if not records:
                break
            
            buffer.extend(records)
            total += len(records)
            
            # Salva part
            if len(buffer) >= batch_size:
                part_num += 1
                part_name = f"{prefix}part-{part_num:05d}.jsonl.gz"
                
                lines = [json.dumps(r, ensure_ascii=False, default=str) for r in buffer]
                content = '\n'.join(lines).encode('utf-8')
                compressed = gzip.compress(content)
                
                file_client = fs.get_file_client(f"{base_path}/{part_name}")
                file_client.upload_data(compressed, overwrite=True)
                
                print(f"[Worker {worker_id}] {prefix}: {part_name} ({len(buffer):,} membros)")
                buffer = []
            
            if len(records) < take:
                break
            
            skip += take
            
        except Exception as e:
            error_msg = str(e)
            if "base64" in error_msg.lower():
                return {"error": f"Azure Key inválida: {error_msg}", "period": f"{start_date} - {end_date}"}
            
            retry_count += 1
            if retry_count > max_retries:
                print(f"[Worker {worker_id}] Erro persistente em {start_date}: {e}")
                return {"error": str(e), "period": f"{start_date} - {end_date}"}
            
            print(f"[Worker {worker_id}] Erro em {start_date}: {e} - retry {retry_count}/{max_retries}")
            time.sleep(5)
            continue
    
    # Salva restante
    if buffer:
        part_num += 1
        part_name = f"{prefix}part-{part_num:05d}.jsonl.gz"
        
        lines = [json.dumps(r, ensure_ascii=False, default=str) for r in buffer]
        content = '\n'.join(lines).encode('utf-8')
        compressed = gzip.compress(content)
        
        file_client = fs.get_file_client(f"{base_path}/{part_name}")
        file_client.upload_data(compressed, overwrite=True)
        
        print(f"[Worker {worker_id}] {prefix}: {part_name} ({len(buffer):,} membros) - FINAL")
    
    elapsed = time.time() - start_time
    
    return {
        "period": f"{start_date} - {end_date}",
        "records": total,
        "parts": part_num,
        "elapsed": elapsed,
        "requests": rate_limiter.count,
        "worker_id": worker_id,
    }


def generate_periods(start_date: str, end_date: str, chunk_months: int = 3) -> List[Tuple[str, str]]:
    """Gera lista de períodos (trimestres por padrão)."""
    from dateutil.relativedelta import relativedelta
    
    periods = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current < end:
        period_end = current + relativedelta(months=chunk_months) - timedelta(days=1)
        if period_end > end:
            period_end = end
        
        periods.append((current.strftime("%Y-%m-%d"), period_end.strftime("%Y-%m-%d")))
        current = current + relativedelta(months=chunk_months)
    
    return periods


def main():
    parser = argparse.ArgumentParser(description="Extrator EVO Members PARALELO")
    parser.add_argument("--start-date", help="Data início (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Data fim (YYYY-MM-DD)")
    parser.add_argument("--year", type=int, help="Extrair ano completo")
    parser.add_argument("--workers", type=int, default=4, help="Workers paralelos")
    parser.add_argument("--chunk-months", type=int, default=3, help="Meses por chunk (default: 3)")
    args = parser.parse_args()
    
    # Valida credenciais
    validate_env()
    
    # Determina período
    if args.year:
        start_date = f"{args.year}-01-01"
        end_date = f"{args.year}-12-31"
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        print("Use --year ou --start-date/--end-date")
        sys.exit(1)
    
    print("="*60)
    print("EVO MEMBERS - EXTRAÇÃO PARALELA")
    print("="*60)
    print(f"Período: {start_date} a {end_date}")
    print(f"Workers: {args.workers}")
    print(f"showMemberships: True (contratos incluídos)")
    print(f"showsResponsibles: True")
    
    # Gera períodos
    periods = generate_periods(start_date, end_date, args.chunk_months)
    print(f"Períodos (chunks de {args.chunk_months} meses): {len(periods)}")
    
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Run ID: {run_id}")
    print("="*60)
    
    # Cria cópia do ENV para passar aos workers (evita problemas de pickle)
    env_copy = {
        "EVO_API_URL": ENV["EVO_API_URL"],
        "EVO_USERNAME": ENV["EVO_USERNAME"],
        "EVO_PASSWORD": ENV["EVO_PASSWORD"],
        "AZURE_STORAGE_ACCOUNT_NAME": ENV["AZURE_STORAGE_ACCOUNT_NAME"],
        "AZURE_STORAGE_ACCOUNT_KEY": ENV["AZURE_STORAGE_ACCOUNT_KEY"],
        "AZURE_CONTAINER_NAME": ENV["AZURE_CONTAINER_NAME"],
    }
    
    # Prepara args - passa env_copy para cada worker
    worker_args = [
        (p[0], p[1], run_id, i % args.workers, env_copy)
        for i, p in enumerate(periods)
    ]
    
    # Executa
    start_time = time.time()
    results = []
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(extract_period_worker, arg): arg for arg in worker_args}
        
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
                
                if "error" not in result:
                    print(f"✓ {result['period']}: {result['records']:,} membros em {result['elapsed']:.0f}s")
                else:
                    print(f"✗ {result['period']}: {result['error']}")
            except Exception as e:
                print(f"✗ Erro fatal: {e}")
    
    total_elapsed = time.time() - start_time
    total_records = sum(r.get("records", 0) for r in results)
    total_parts = sum(r.get("parts", 0) for r in results)
    
    # Salva manifest
    from azure.storage.filedatalake import DataLakeServiceClient
    service = DataLakeServiceClient(
        account_url=f"https://{ENV['AZURE_STORAGE_ACCOUNT_NAME']}.dfs.core.windows.net",
        credential=ENV["AZURE_STORAGE_ACCOUNT_KEY"]
    )
    fs = service.get_file_system_client(ENV["AZURE_CONTAINER_NAME"])
    
    base_path = f"bronze/evo/entity=members/ingestion_date={datetime.now(timezone.utc).strftime('%Y-%m-%d')}/run_id={run_id}"
    
    manifest = {
        "entity": "members",
        "mode": "full_parallel",
        "run_id": run_id,
        "start_date": start_date,
        "end_date": end_date,
        "workers": args.workers,
        "show_memberships": True,
        "total_records": total_records,
        "total_parts": total_parts,
        "total_elapsed_seconds": total_elapsed,
        "periods": len(periods),
        "results": results,
    }
    
    content = json.dumps(manifest, indent=2, ensure_ascii=False, default=str)
    fs.get_file_client(f"{base_path}/_manifest.json").upload_data(content.encode('utf-8'), overwrite=True)
    
    print()
    print("="*60)
    print("RESULTADO")
    print("="*60)
    print(f"Total membros: {total_records:,}")
    print(f"Total parts: {total_parts}")
    print(f"Tempo total: {total_elapsed/60:.1f} min ({total_elapsed/3600:.1f}h)")
    if total_elapsed > 0:
        print(f"Taxa: {total_records/total_elapsed:.0f} membros/segundo")
    print(f"Run ID: {run_id}")
    
    return manifest


if __name__ == "__main__":
    main()
