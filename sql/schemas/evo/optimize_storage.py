# -*- coding: utf-8 -*-
"""
Otimizador de Storage PostgreSQL

Analisa e otimiza tabelas do data lake.

Uso:
    cd C:/skyfit-datalake
    
    # Apenas análise (não modifica nada)
    python sql/optimize_storage.py --analyze
    
    # VACUUM ANALYZE em todas as tabelas
    python sql/optimize_storage.py --vacuum
    
    # REINDEX (reconstruir índices bloated)
    python sql/optimize_storage.py --reindex
    
    # Tudo junto (recomendado em janela de manutenção)
    python sql/optimize_storage.py --vacuum --reindex
"""
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# Setup env
def load_env():
    env_paths = [
        Path(r"C:\skyfit-datalake\config\.env"),
        Path("config/.env"),
        Path(".env"),
    ]
    try:
        from dotenv import load_dotenv
        for p in env_paths:
            if p.exists():
                load_dotenv(p, override=True)
                return True
    except ImportError:
        pass
    return False

load_env()

import psycopg2

ENV = {
    "PG_HOST": os.environ.get("PG_HOST"),
    "PG_PORT": os.environ.get("PG_PORT", "5432"),
    "PG_DATABASE": os.environ.get("PG_DATABASE", "postgres"),
    "PG_USER": os.environ.get("PG_USER"),
    "PG_PASSWORD": os.environ.get("PG_PASSWORD"),
    "PG_SSLMODE": os.environ.get("PG_SSLMODE", "require"),
}


def get_connection():
    return psycopg2.connect(
        host=ENV["PG_HOST"],
        port=ENV["PG_PORT"],
        database=ENV["PG_DATABASE"],
        user=ENV["PG_USER"],
        password=ENV["PG_PASSWORD"],
        sslmode=ENV["PG_SSLMODE"],
    )


def format_bytes(size):
    """Formata bytes para human readable."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(size) < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"


def analyze_tables(conn):
    """Analisa tamanho e bloat das tabelas."""
    print("\n" + "="*80)
    print("📊 ANÁLISE DE ARMAZENAMENTO")
    print("="*80)
    
    cur = conn.cursor()
    
    # Tamanho das tabelas
    cur.execute("""
        SELECT 
            schemaname || '.' || tablename AS tabela,
            pg_total_relation_size(schemaname || '.' || tablename) AS total_bytes,
            pg_relation_size(schemaname || '.' || tablename) AS data_bytes,
            pg_total_relation_size(schemaname || '.' || tablename) - pg_relation_size(schemaname || '.' || tablename) AS index_bytes
        FROM pg_tables 
        WHERE schemaname IN ('stg_evo', 'core')
        ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
    """)
    
    tables = cur.fetchall()
    total_size = 0
    total_index = 0
    
    print(f"\n{'Tabela':<40} {'Total':>12} {'Dados':>12} {'Índices':>12} {'% Índice':>10}")
    print("-" * 86)
    
    problematic = []
    
    for table, total, data, index in tables:
        total_size += total
        total_index += index
        
        pct_index = (index / total * 100) if total > 0 else 0
        
        # Marca como problemático se índice > 50% do total
        flag = "⚠️" if pct_index > 50 else ""
        if pct_index > 50:
            problematic.append((table, total, data, index, pct_index))
        
        print(f"{table:<40} {format_bytes(total):>12} {format_bytes(data):>12} {format_bytes(index):>12} {pct_index:>8.1f}% {flag}")
    
    print("-" * 86)
    print(f"{'TOTAL':<40} {format_bytes(total_size):>12} {format_bytes(total_size - total_index):>12} {format_bytes(total_index):>12}")
    
    # Diagnóstico
    print("\n" + "="*80)
    print("🔍 DIAGNÓSTICO")
    print("="*80)
    
    if problematic:
        print("\n⚠️ TABELAS COM ÍNDICES BLOATED (>50% do espaço é índice):")
        for table, total, data, index, pct in problematic:
            saving = index - data  # Economia potencial
            print(f"  • {table}")
            print(f"    - Total: {format_bytes(total)} | Dados: {format_bytes(data)} | Índices: {format_bytes(index)}")
            print(f"    - Economia potencial após REINDEX: ~{format_bytes(saving)}")
    else:
        print("\n✅ Nenhuma tabela com índices excessivamente bloated")
    
    # Tuplas mortas
    cur.execute("""
        SELECT 
            schemaname || '.' || relname AS tabela,
            n_dead_tup,
            n_live_tup,
            last_vacuum,
            last_autovacuum
        FROM pg_stat_user_tables
        WHERE schemaname IN ('stg_evo', 'core')
          AND n_dead_tup > 10000
        ORDER BY n_dead_tup DESC
        LIMIT 10
    """)
    
    dead_tuples = cur.fetchall()
    
    if dead_tuples:
        print("\n⚠️ TABELAS COM MUITAS TUPLAS MORTAS (precisam de VACUUM):")
        for table, dead, live, vac, autovac in dead_tuples:
            pct = (dead / (live + dead) * 100) if (live + dead) > 0 else 0
            print(f"  • {table}: {dead:,} mortas ({pct:.1f}%)")
    else:
        print("\n✅ Nenhuma tabela com excesso de tuplas mortas")
    
    # Índices não utilizados
    cur.execute("""
        SELECT 
            schemaname || '.' || tablename AS tabela,
            indexname,
            pg_relation_size(indexrelid) AS size
        FROM pg_stat_user_indexes
        JOIN pg_indexes ON indexrelname = indexname
        WHERE schemaname IN ('stg_evo', 'core')
          AND idx_scan = 0
          AND indexname NOT LIKE '%_pkey'
        ORDER BY pg_relation_size(indexrelid) DESC
        LIMIT 10
    """)
    
    unused_idx = cur.fetchall()
    
    if unused_idx:
        print("\n⚠️ ÍNDICES NUNCA UTILIZADOS (candidatos a remoção):")
        for table, idx, size in unused_idx:
            print(f"  • {idx} em {table}: {format_bytes(size)}")
    
    cur.close()
    
    return problematic


def vacuum_tables(conn):
    """Executa VACUUM ANALYZE nas tabelas principais."""
    print("\n" + "="*80)
    print("🧹 VACUUM ANALYZE")
    print("="*80)
    
    tables = [
        'stg_evo.sales_raw',
        'stg_evo.members_raw',
        'stg_evo.prospects_raw',
        'core.evo_sales',
        'core.evo_sale_items',
        'core.evo_receivables',
        'core.evo_members',
        'core.evo_member_memberships',
        'core.evo_member_contacts',
        'core.evo_prospects',
    ]
    
    conn.autocommit = True  # VACUUM precisa de autocommit
    cur = conn.cursor()
    
    for table in tables:
        try:
            print(f"\n  Processando {table}...", end=" ", flush=True)
            start = datetime.now()
            cur.execute(f"VACUUM (ANALYZE) {table}")
            elapsed = (datetime.now() - start).total_seconds()
            print(f"✅ ({elapsed:.1f}s)")
        except Exception as e:
            print(f"❌ Erro: {e}")
    
    cur.close()
    conn.autocommit = False


def reindex_tables(conn):
    """Reconstrói índices das tabelas principais."""
    print("\n" + "="*80)
    print("🔧 REINDEX (CONCURRENTLY)")
    print("="*80)
    print("⚠️ Este processo pode demorar bastante para tabelas grandes!")
    
    # Pega lista de índices grandes
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            schemaname,
            tablename,
            indexname,
            pg_relation_size(indexrelid) AS size
        FROM pg_stat_user_indexes
        JOIN pg_indexes ON indexrelname = indexname
        WHERE schemaname IN ('stg_evo', 'core')
        ORDER BY pg_relation_size(indexrelid) DESC
        LIMIT 20
    """)
    
    indexes = cur.fetchall()
    cur.close()
    
    conn.autocommit = True  # REINDEX CONCURRENTLY precisa de autocommit
    cur = conn.cursor()
    
    for schema, table, idx, size in indexes:
        if size < 100 * 1024 * 1024:  # Pula índices < 100MB
            continue
        
        try:
            print(f"\n  Reconstruindo {idx} ({format_bytes(size)})...", end=" ", flush=True)
            start = datetime.now()
            cur.execute(f"REINDEX INDEX CONCURRENTLY {schema}.{idx}")
            elapsed = (datetime.now() - start).total_seconds()
            print(f"✅ ({elapsed:.1f}s)")
        except Exception as e:
            print(f"❌ Erro: {e}")
    
    cur.close()
    conn.autocommit = False


def show_recommendations(problematic):
    """Mostra recomendações baseadas na análise."""
    print("\n" + "="*80)
    print("💡 RECOMENDAÇÕES")
    print("="*80)
    
    total_saving = sum(idx - data for _, _, data, idx, _ in problematic) if problematic else 0
    
    if total_saving > 50 * 1024**3:  # > 50GB
        print("""
1. AÇÃO IMEDIATA: Execute REINDEX nas tabelas problemáticas
   
   Comandos:
   python sql/optimize_storage.py --vacuum --reindex
   
   Ou manualmente no SQL:
   REINDEX INDEX CONCURRENTLY stg_evo.sales_raw_pkey;
   VACUUM (ANALYZE) stg_evo.sales_raw;
""")
    
    print("""
2. CONFIGURAÇÃO DO AZURE:
   - Habilite "Storage Auto-grow" no Azure Portal
   - Configure maintenance_work_mem = 1GB para VACUUMs mais rápidos
   
3. MANUTENÇÃO REGULAR:
   - Agende VACUUM ANALYZE semanal
   - Monitore pg_stat_user_tables para tuplas mortas
   
4. ALTERNATIVAS PARA ECONOMIA DE ESPAÇO:
   - Considere particionar tabelas grandes (entries já é particionada)
   - Para STG: Mantenha apenas último mês, arquive o resto no Bronze
   - Use pg_repack se disponível (otimiza sem lock)
""")
    
    if total_saving > 0:
        print(f"\n📊 ECONOMIA POTENCIAL ESTIMADA: {format_bytes(total_saving)}")


def main():
    parser = argparse.ArgumentParser(description="Otimizador de Storage PostgreSQL")
    parser.add_argument("--analyze", action="store_true", help="Apenas análise (não modifica)")
    parser.add_argument("--vacuum", action="store_true", help="Executa VACUUM ANALYZE")
    parser.add_argument("--reindex", action="store_true", help="Reconstrói índices bloated")
    args = parser.parse_args()
    
    # Se nenhuma flag, faz apenas análise
    if not args.vacuum and not args.reindex:
        args.analyze = True
    
    print("\n" + "="*80)
    print("🔧 SKYFIT DATA LAKE - OTIMIZADOR DE STORAGE")
    print("="*80)
    print(f"Host: {ENV['PG_HOST']}")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    conn = get_connection()
    
    try:
        # Sempre faz análise primeiro
        problematic = analyze_tables(conn)
        
        if args.vacuum:
            vacuum_tables(conn)
        
        if args.reindex:
            reindex_tables(conn)
        
        # Mostra tamanho após otimizações
        if args.vacuum or args.reindex:
            print("\n" + "="*80)
            print("📊 RESULTADO APÓS OTIMIZAÇÃO")
            print("="*80)
            analyze_tables(conn)
        
        show_recommendations(problematic)
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
