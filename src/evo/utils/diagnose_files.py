# -*- coding: utf-8 -*-
"""
Diagnóstico de Estrutura - Mostra os arquivos reais no Azure

Uso:
    cd C:/skyfit-datalake/evo_entries
    python src/utils/diagnose_files.py
"""
import os
import sys
from collections import defaultdict
from pathlib import Path

def load_env():
    env_paths = [
        Path(r"C:\skyfit-datalake\config\.env"),
        Path("config/.env"),
    ]
    try:
        from dotenv import load_dotenv
        for p in env_paths:
            if p.exists():
                load_dotenv(p, override=True)
                print(f"[ENV] Carregado: {p}")
                return True
    except ImportError:
        pass
    return False

load_env()

ENV = {
    "AZURE_STORAGE_ACCOUNT_NAME": os.environ.get("AZURE_STORAGE_ACCOUNT") or os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
    "AZURE_STORAGE_ACCOUNT_KEY": os.environ.get("AZURE_STORAGE_KEY") or os.environ.get("AZURE_STORAGE_ACCOUNT_KEY"),
    "AZURE_CONTAINER_NAME": os.environ.get("ADLS_CONTAINER") or os.environ.get("AZURE_CONTAINER_NAME", "datalake"),
}


def diagnose():
    from azure.storage.filedatalake import DataLakeServiceClient
    
    print("\n" + "="*80)
    print("DIAGNÓSTICO DE ESTRUTURA DE ARQUIVOS")
    print("="*80)
    
    service = DataLakeServiceClient(
        account_url=f"https://{ENV['AZURE_STORAGE_ACCOUNT_NAME']}.dfs.core.windows.net",
        credential=ENV["AZURE_STORAGE_ACCOUNT_KEY"]
    )
    fs = service.get_file_system_client(ENV["AZURE_CONTAINER_NAME"])
    
    base_path = "bronze/evo/entity=entries"
    
    print(f"\nListando: {base_path}")
    print("-" * 80)
    
    # Coleta arquivos
    all_files = []
    for item in fs.get_paths(path=base_path, recursive=True):
        if not item.is_directory:
            all_files.append({
                'path': item.name,
                'size': item.content_length or 0
            })
    
    print(f"\nTotal de arquivos: {len(all_files)}")
    
    if not all_files:
        print("❌ Nenhum arquivo encontrado!")
        return
    
    # Mostra primeiros e últimos arquivos
    print("\n📁 PRIMEIROS 10 ARQUIVOS:")
    print("-" * 80)
    for f in all_files[:10]:
        print(f"  {f['path']}")
        print(f"    Size: {f['size'] / 1024:.1f} KB")
    
    print("\n📁 ÚLTIMOS 10 ARQUIVOS:")
    print("-" * 80)
    for f in all_files[-10:]:
        print(f"  {f['path']}")
        print(f"    Size: {f['size'] / 1024:.1f} KB")
    
    # Analisa estrutura de diretórios
    print("\n📂 ESTRUTURA DE DIRETÓRIOS:")
    print("-" * 80)
    
    dirs = defaultdict(int)
    for f in all_files:
        parts = f['path'].split('/')
        # Pega até o nível de ingestion_date
        for i, part in enumerate(parts):
            if 'ingestion_date=' in part or 'run_id=' in part:
                dirs['/'.join(parts[:i+1])] += 1
    
    # Mostra run_ids únicos
    run_ids = set()
    ingestion_dates = set()
    
    for f in all_files:
        path = f['path']
        if 'run_id=' in path:
            for part in path.split('/'):
                if part.startswith('run_id='):
                    run_ids.add(part.replace('run_id=', ''))
        if 'ingestion_date=' in path:
            for part in path.split('/'):
                if part.startswith('ingestion_date='):
                    ingestion_dates.add(part.replace('ingestion_date=', ''))
    
    print(f"\n📅 RUN IDs encontrados: {len(run_ids)}")
    for rid in sorted(run_ids):
        count = sum(1 for f in all_files if f'run_id={rid}' in f['path'])
        print(f"  • {rid}: {count} arquivos")
    
    print(f"\n📅 INGESTION DATES encontrados: {len(ingestion_dates)}")
    for date in sorted(ingestion_dates)[:20]:  # Mostra primeiros 20
        count = sum(1 for f in all_files if f'ingestion_date={date}' in f['path'])
        print(f"  • {date}: {count} arquivos")
    
    if len(ingestion_dates) > 20:
        print(f"  ... e mais {len(ingestion_dates) - 20} datas")
        print(f"\n  Última data: {sorted(ingestion_dates)[-1]}")
    
    # Tamanho total
    total_size = sum(f['size'] for f in all_files)
    print(f"\n📊 TAMANHO TOTAL: {total_size / (1024**3):.2f} GB")
    
    # Recomendação
    print("\n" + "="*80)
    print("📍 RECOMENDAÇÃO")
    print("="*80)
    
    if ingestion_dates:
        last_date = sorted(ingestion_dates)[-1]
        print(f"\nÚltima data extraída: {last_date}")
        
        # Calcula próxima data
        from datetime import datetime, timedelta
        try:
            last_dt = datetime.strptime(last_date, "%Y-%m-%d")
            next_dt = last_dt + timedelta(days=1)
            today = datetime.now().strftime("%Y-%m-%d")
            
            print(f"\nComando para continuar:")
            print(f"""
cd C:\\skyfit-datalake\\evo_entries
python src/extractors/evo_entries_bronze_parallel.py --start-date {next_dt.strftime('%Y-%m-%d')} --end-date {today} --workers 8
""")
        except:
            pass


if __name__ == "__main__":
    diagnose()
