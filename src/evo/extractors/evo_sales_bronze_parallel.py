# -*- coding: utf-8 -*-
"""
Extractor PARALELO: EVO Sales → Bronze

Extrai múltiplos períodos simultaneamente para acelerar drasticamente.

ESTRATÉGIA:
- Divide o período em chunks (meses)
- Roda N workers em paralelo, cada um extraindo um mês diferente
- Cada worker tem seu próprio rate limiter

Performance:
- Serial: ~4 min/5k registros = 133h para 10M
- Paralelo (4 workers): ~33h para 10M
- Paralelo na janela livre (0h-5h, 8 workers): ~8h para 10M

Uso:
    cd C:/skyfit-datalake/evo_sales
    
    # 4 workers paralelos (padrão)
    python src/extractors/evo_sales_bronze_parallel.py --start-date 2024-01-01 --end-date 2026-01-09 --workers 4
    
    # 8 workers (janela livre 0h-5h)
    python src/extractors/evo_sales_bronze_parallel.py --start-date 2024-01-01 --end-date 2026-01-09 --workers 8
    
    # Apenas um ano
    python src/extractors/evo_sales_bronze_parallel.py --year 2024 --workers 4
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
from multiprocessing import Manager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# Desabilita logging verboso do Azure SDK
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)


def load_env():
    """Carrega .env."""
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
                load_dotenv(p, override=False)
                return True
    except ImportError:
        pass
    return False


load_env()

ENV = {
    "EVO_API_URL": os.environ.get("EVO_API_URL", "https://evo-integracao.w12app.com.br"),
    "EVO_USERNAME": os.environ.get("EVO_USERNAME"),
    "EVO_PASSWORD": os.environ.get("EVO_PASSWORD"),
    "AZURE_STORAGE_ACCOUNT_NAME": os.environ.get("AZURE_STORAGE_ACCOUNT") or os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
    "AZURE_STORAGE_ACCOUNT_KEY": os.environ.get("AZURE_STORAGE_KEY") or os.environ.get("AZURE_STORAGE_ACCOUNT_KEY"),
    "AZURE_CONTAINER_NAME": os.environ.get("ADLS_CONTAINER") or os.environ.get("AZURE_CONTAINER_NAME", "datalake"),
}


class RateLimiter:
    """Rate limiter por worker."""
    
    def __init__(self, rpm: int = 40):
        self.min_interval = 60.0 / rpm
        self.last_request = 0
        self.count = 0
    
    def wait(self):
        # Janela livre: sem limite
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
    Roda em processo separado.
    """
    start_date, end_date, run_id, show_receivables, worker_id = args
    
    from azure.storage.filedatalake import DataLakeServiceClient
    
    # Setup
    rate_limiter = RateLimiter(rpm=40)
    session = requests.Session()
    session.auth = (ENV["EVO_USERNAME"], ENV["EVO_PASSWORD"])
    
    # Azure client
    service = DataLakeServiceClient(
        account_url=f"https://{ENV['AZURE_STORAGE_ACCOUNT_NAME']}.dfs.core.windows.net",
        credential=ENV["AZURE_STORAGE_ACCOUNT_KEY"]
    )
    fs = service.get_file_system_client(ENV["AZURE_CONTAINER_NAME"])
    
    base_path = f"bronze/evo/entity=sales/ingestion_date={datetime.now(timezone.utc).strftime('%Y-%m-%d')}/run_id={run_id}"
    
    # Extração
    skip = 0
    take = 100
    buffer = []
    part_num = 0
    total = 0
    batch_size = 5000
    prefix = f"{start_date[:7]}_"
    
    start_time = time.time()
    
    while True:
        try:
            rate_limiter.wait()
            
            url = f"{ENV['EVO_API_URL']}/api/v2/sales"
            params = {
                "skip": skip,
                "take": take,
                "showReceivables": str(show_receivables).lower(),
                "dateSaleStart": start_date,
                "dateSaleEnd": end_date,
            }
            
            resp = session.get(url, params=params, timeout=120)
            
            if resp.status_code == 401:
                return {"error": "401 Unauthorized", "period": f"{start_date} - {end_date}"}
            
            resp.raise_for_status()
            data = resp.json()
            
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
                
                print(f"[Worker {worker_id}] {prefix}: {part_name} ({len(buffer):,} registros)")
                buffer = []
            
            if len(records) < take:
                break
            
            skip += take
            
        except Exception as e:
            print(f"[Worker {worker_id}] Erro em {start_date}: {e}")
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
        
        print(f"[Worker {worker_id}] {prefix}: {part_name} ({len(buffer):,} registros) - FINAL")
    
    elapsed = time.time() - start_time
    
    return {
        "period": f"{start_date} - {end_date}",
        "records": total,
        "parts": part_num,
        "elapsed": elapsed,
        "requests": rate_limiter.count,
        "worker_id": worker_id,
    }


def generate_periods(start_date: str, end_date: str) -> List[Tuple[str, str]]:
    """Gera lista de períodos mensais."""
    from dateutil.relativedelta import relativedelta
    
    periods = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current < end:
        period_end = current + relativedelta(months=1) - timedelta(days=1)
        if period_end > end:
            period_end = end
        
        periods.append((current.strftime("%Y-%m-%d"), period_end.strftime("%Y-%m-%d")))
        current = current + relativedelta(months=1)
    
    return periods


def main():
    parser = argparse.ArgumentParser(description="Extrator EVO Sales PARALELO")
    parser.add_argument("--start-date", help="Data início (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Data fim (YYYY-MM-DD)")
    parser.add_argument("--year", type=int, help="Extrair ano completo")
    parser.add_argument("--workers", type=int, default=4, help="Workers paralelos (default: 4)")
    parser.add_argument("--no-receivables", action="store_true")
    args = parser.parse_args()
    
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
    print("EVO SALES - EXTRAÇÃO PARALELA")
    print("="*60)
    print(f"Período: {start_date} a {end_date}")
    print(f"Workers: {args.workers}")
    print(f"Receivables: {not args.no_receivables}")
    
    # Gera períodos
    periods = generate_periods(start_date, end_date)
    print(f"Períodos (meses): {len(periods)}")
    
    # Run ID único
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Run ID: {run_id}")
    print("="*60)
    
    # Prepara args para workers
    worker_args = [
        (p[0], p[1], run_id, not args.no_receivables, i % args.workers)
        for i, p in enumerate(periods)
    ]
    
    # Executa em paralelo
    start_time = time.time()
    results = []
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(extract_period_worker, arg): arg for arg in worker_args}
        
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
                
                if "error" not in result:
                    print(f"✓ {result['period']}: {result['records']:,} registros em {result['elapsed']:.0f}s")
                else:
                    print(f"✗ {result['period']}: {result['error']}")
                    
            except Exception as e:
                print(f"✗ Erro: {e}")
    
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
    
    base_path = f"bronze/evo/entity=sales/ingestion_date={datetime.now(timezone.utc).strftime('%Y-%m-%d')}/run_id={run_id}"
    
    manifest = {
        "entity": "sales",
        "mode": "full_parallel",
        "run_id": run_id,
        "start_date": start_date,
        "end_date": end_date,
        "workers": args.workers,
        "total_records": total_records,
        "total_parts": total_parts,
        "total_elapsed_seconds": total_elapsed,
        "periods": len(periods),
        "results": results,
    }
    
    content = json.dumps(manifest, indent=2, ensure_ascii=False, default=str)
    fs.get_file_client(f"{base_path}/_manifest.json").upload_data(content.encode('utf-8'), overwrite=True)
    
    # Resultado
    print()
    print("="*60)
    print("RESULTADO")
    print("="*60)
    print(f"Total registros: {total_records:,}")
    print(f"Total parts: {total_parts}")
    print(f"Tempo total: {total_elapsed/60:.1f} min ({total_elapsed/3600:.1f}h)")
    print(f"Taxa: {total_records/total_elapsed:.0f} registros/segundo")
    print(f"Run ID: {run_id}")
    
    return manifest


if __name__ == "__main__":
    main()
