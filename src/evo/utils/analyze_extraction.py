# -*- coding: utf-8 -*-
"""
Analisador de Extração: Identifica onde a extração parou e gaps

Uso:
    cd C:/skyfit-datalake/evo_entries
    python src/utils/analyze_extraction.py
"""
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# Setup
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent

def load_env():
    env_paths = [
        PROJECT_ROOT / "config" / ".env",
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
        pass
    return False

load_env()

ENV = {
    "AZURE_STORAGE_ACCOUNT_NAME": os.environ.get("AZURE_STORAGE_ACCOUNT") or os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
    "AZURE_STORAGE_ACCOUNT_KEY": os.environ.get("AZURE_STORAGE_KEY") or os.environ.get("AZURE_STORAGE_ACCOUNT_KEY"),
    "AZURE_CONTAINER_NAME": os.environ.get("ADLS_CONTAINER") or os.environ.get("AZURE_CONTAINER_NAME", "datalake"),
}


def analyze_entries_extraction():
    """Analisa a extração de entries e identifica gaps."""
    from azure.storage.filedatalake import DataLakeServiceClient
    
    print("\n" + "="*70)
    print("ANÁLISE DA EXTRAÇÃO DE ENTRIES")
    print("="*70)
    
    # Conecta ao Azure
    service = DataLakeServiceClient(
        account_url=f"https://{ENV['AZURE_STORAGE_ACCOUNT_NAME']}.dfs.core.windows.net",
        credential=ENV["AZURE_STORAGE_ACCOUNT_KEY"]
    )
    fs = service.get_file_system_client(ENV["AZURE_CONTAINER_NAME"])
    
    # Lista todos os arquivos de entries
    base_path = "bronze/evo/entity=entries"
    print(f"\nListando arquivos em: {base_path}")
    
    files_by_period = defaultdict(list)
    run_ids = set()
    total_files = 0
    total_size = 0
    
    for item in fs.get_paths(path=base_path, recursive=True):
        if item.is_directory:
            continue
        if not item.name.endswith('.jsonl.gz'):
            continue
        
        total_files += 1
        total_size += item.content_length or 0
        
        # Extrai run_id
        run_match = re.search(r"run_id=([^/]+)", item.name)
        if run_match:
            run_ids.add(run_match.group(1))
        
        # Extrai período do nome do arquivo (ex: 2024-01_part-00001.jsonl.gz)
        # ou ingestion_date
        period_match = re.search(r"(\d{4}-\d{2})_part", item.name)
        if period_match:
            period = period_match.group(1)
            files_by_period[period].append({
                'name': item.name,
                'size': item.content_length or 0
            })
    
    print(f"\n📊 RESUMO:")
    print(f"  Total de arquivos: {total_files:,}")
    print(f"  Tamanho total: {total_size / (1024**3):.2f} GB")
    print(f"  Run IDs: {sorted(run_ids)}")
    
    # Analisa períodos - detecta formato YYYY-MM no nome do arquivo
    print(f"\n📅 PERÍODOS EXTRAÍDOS:")
    print("-" * 50)
    
    periods_sorted = sorted(files_by_period.keys())
    
    if not periods_sorted:
        # Tenta extrair do nome do arquivo diretamente
        print("  Tentando extrair período do nome do arquivo...")
        for f in all_files[:5]:
            # Extrai YYYY-MM do nome (ex: 2024-12_part-00001.jsonl.gz)
            name = Path(f['name']).name
            period_match = re.search(r"(\d{4}-\d{2})_part", name)
            if period_match:
                period = period_match.group(1)
                files_by_period[period].append(f)
        
        # Também tenta pelo path completo
        for f in all_files:
            name = f['path'] if isinstance(f, dict) else f
            # Tenta vários padrões
            patterns = [
                r"(\d{4}-\d{2})_part",  # 2024-12_part
                r"(\d{4}-\d{2})/",       # /2024-12/
                r"period=(\d{4}-\d{2})", # period=2024-12
            ]
            for pattern in patterns:
                match = re.search(pattern, name)
                if match:
                    files_by_period[match.group(1)].append(f)
                    break
        
        periods_sorted = sorted(files_by_period.keys())
    
    if not periods_sorted:
        print("  ❌ Não foi possível identificar períodos!")
        print("\n  Estrutura dos arquivos:")
        for f in all_files[:10]:
            print(f"    {f['path']}")
        return
    
    first_period = periods_sorted[0]
    last_period = periods_sorted[-1]
    
    print(f"  Primeiro período: {first_period}")
    print(f"  Último período:   {last_period}")
    print(f"  Total de meses:   {len(periods_sorted)}")
    
    # Estatísticas por período
    print(f"\n📈 DETALHAMENTO POR PERÍODO:")
    print("-" * 50)
    print(f"{'Período':<10} {'Arquivos':>10} {'Tamanho':>12} {'Status'}")
    print("-" * 50)
    
    # Gera todos os meses esperados
    start_year, start_month = map(int, first_period.split('-'))
    end_year, end_month = map(int, last_period.split('-'))
    
    current = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)
    
    expected_periods = []
    while current <= end:
        expected_periods.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)
    
    gaps = []
    for period in expected_periods:
        if period in files_by_period:
            files = files_by_period[period]
            size_mb = sum(f['size'] for f in files) / (1024**2)
            status = "✅"
            print(f"  {period:<10} {len(files):>10} {size_mb:>10.1f} MB {status}")
        else:
            gaps.append(period)
            print(f"  {period:<10} {'0':>10} {'0':>10} MB ❌ FALTANDO")
    
    # Identifica o ponto de parada
    print(f"\n🔍 ANÁLISE DE GAPS:")
    print("-" * 50)
    
    if gaps:
        print(f"  ⚠️ Períodos faltantes: {len(gaps)}")
        for gap in gaps:
            print(f"    - {gap}")
        
        # Identifica onde continuar
        if gaps[0] > last_period:
            # Parou no meio e há gaps depois
            print(f"\n  📍 RECOMENDAÇÃO:")
            print(f"     Continuar extração a partir de: {gaps[0]}-01")
        else:
            # Há gaps no meio
            print(f"\n  📍 RECOMENDAÇÃO:")
            print(f"     Extrair períodos faltantes: {gaps}")
    else:
        print("  ✅ Nenhum gap encontrado!")
    
    # Análise do último período
    if last_period in files_by_period:
        last_files = files_by_period[last_period]
        print(f"\n📍 ÚLTIMO PERÍODO ({last_period}):")
        print(f"   Arquivos: {len(last_files)}")
        
        # Verifica se parece completo (estimativa baseada em ~50k entries/dia)
        # Um mês completo teria ~30 dias * 50k = 1.5M entries
        # Com ~10k entries por arquivo, seriam ~150 arquivos por mês
        if len(last_files) < 50:
            print(f"   ⚠️ Parece INCOMPLETO (poucos arquivos)")
            print(f"   📍 RECOMENDAÇÃO: Continuar de {last_period}-01")
        else:
            print(f"   ✅ Parece completo")
            
            # Próximo mês
            year, month = map(int, last_period.split('-'))
            if month == 12:
                next_period = f"{year+1}-01"
            else:
                next_period = f"{year}-{month+1:02d}"
            print(f"   📍 Próximo período: {next_period}")
    
    # Comando sugerido
    print(f"\n" + "="*70)
    print("COMANDO SUGERIDO PARA CONTINUAR:")
    print("="*70)
    
    if gaps:
        start_date = f"{gaps[0]}-01"
    else:
        # Continua do último período
        year, month = map(int, last_period.split('-'))
        if month == 12:
            start_date = f"{year+1}-01-01"
        else:
            start_date = f"{year}-{month+1:02d}-01"
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    print(f"""
cd C:\\skyfit-datalake\\evo_entries
python src/extractors/evo_entries_bronze_parallel.py --start-date {start_date} --end-date {today} --workers 8
""")


if __name__ == "__main__":
    analyze_entries_extraction()
