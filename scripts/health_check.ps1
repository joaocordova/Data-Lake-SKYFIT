# ============================================================================
# SKYFIT DATA LAKE - HEALTH CHECK
# ============================================================================
# Verifica integridade dos dados nas tabelas CORE
# Uso: .\scripts\health_check.ps1
# ============================================================================

$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

# Ativar venv
$venvActivate = Join-Path $ProjectRoot ".venv\Scripts\Activate.ps1"
if (Test-Path $venvActivate) {
    & $venvActivate
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "SKYFIT DATA LAKE - HEALTH CHECK" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Executar health check via Python
$pythonCode = @"
import sys
sys.path.insert(0, '.')

from config.settings import postgres
import psycopg2
from datetime import datetime

def fmt_num(n):
    return '{:,}'.format(n).replace(',', '.')

try:
    conn = psycopg2.connect(
        host=postgres.HOST,
        port=postgres.PORT,
        dbname=postgres.DATABASE,
        user=postgres.USER,
        password=postgres.PASSWORD,
        sslmode=postgres.SSLMODE
    )
    cur = conn.cursor()
    
    # === TABELAS CORE ===
    print('=== TABELAS CORE - CONTAGEM ===')
    print('{:<30} {:>12}'.format('Tabela', 'Registros'))
    print('-' * 45)
    
    tables = [
        ('core.pd_pipelines', 'Pipedrive Pipelines'),
        ('core.pd_stages', 'Pipedrive Stages'),
        ('core.pd_users', 'Pipedrive Users'),
        ('core.pd_organizations', 'Pipedrive Organizations'),
        ('core.pd_persons', 'Pipedrive Persons'),
        ('core.pd_deals', 'Pipedrive Deals'),
        ('core.pd_activities', 'Pipedrive Activities'),
        ('core.zd_organizations', 'Zendesk Organizations'),
        ('core.zd_users', 'Zendesk Users'),
        ('core.zd_groups', 'Zendesk Groups'),
        ('core.zd_ticket_fields', 'Zendesk Ticket Fields'),
        ('core.zd_ticket_forms', 'Zendesk Ticket Forms'),
        ('core.zd_tickets', 'Zendesk Tickets'),
        ('core.zd_ticket_tags', 'Zendesk Ticket Tags'),
        ('core.zd_ticket_custom_fields', 'Zendesk Custom Fields'),
    ]
    
    total = 0
    for table, name in tables:
        try:
            cur.execute(f'SELECT COUNT(*) FROM {table}')
            count = cur.fetchone()[0]
            total += count
            print('{:<30} {:>12}'.format(name, fmt_num(count)))
        except Exception as e:
            print('{:<30} {:>12}'.format(name, 'ERRO'))
    
    print('-' * 45)
    print('{:<30} {:>12}'.format('TOTAL', fmt_num(total)))
    
    # === ULTIMA ATUALIZAÇÃO ===
    print()
    print('=== ULTIMA ATUALIZACAO ===')
    print('{:<25} {:>25}'.format('Tabela', 'Ultima Carga'))
    print('-' * 52)
    
    update_checks = [
        ('core.pd_deals', '_updated_at'),
        ('core.pd_activities', '_updated_at'),
        ('core.zd_tickets', '_updated_at'),
    ]
    
    for table, col in update_checks:
        try:
            cur.execute(f'SELECT MAX({col}) FROM {table}')
            result = cur.fetchone()[0]
            if result:
                print('{:<25} {:>25}'.format(table.replace('core.', ''), str(result)[:25]))
            else:
                print('{:<25} {:>25}'.format(table.replace('core.', ''), 'N/A'))
        except:
            print('{:<25} {:>25}'.format(table.replace('core.', ''), 'ERRO'))
    
    # === VERIFICAÇÃO DE DUPLICATAS ===
    print()
    print('=== VERIFICACAO DE INTEGRIDADE ===')
    
    # Duplicatas Pipedrive
    cur.execute('SELECT COUNT(*) - COUNT(DISTINCT (deal_id, scope)) FROM core.pd_deals')
    dup_deals = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*) - COUNT(DISTINCT (person_id, scope)) FROM core.pd_persons')
    dup_persons = cur.fetchone()[0]
    
    # Duplicatas Zendesk
    cur.execute('SELECT COUNT(*) - COUNT(DISTINCT ticket_id) FROM core.zd_tickets')
    dup_tickets = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*) - COUNT(DISTINCT user_id) FROM core.zd_users')
    dup_users = cur.fetchone()[0]
    
    if dup_deals == 0 and dup_persons == 0 and dup_tickets == 0 and dup_users == 0:
        print('[OK] Sem duplicatas detectadas')
    else:
        print('[WARN] Duplicatas encontradas:')
        if dup_deals > 0: print(f'  - pd_deals: {dup_deals}')
        if dup_persons > 0: print(f'  - pd_persons: {dup_persons}')
        if dup_tickets > 0: print(f'  - zd_tickets: {dup_tickets}')
        if dup_users > 0: print(f'  - zd_users: {dup_users}')
    
    # === DISTRIBUIÇÃO POR SCOPE ===
    print()
    print('=== PIPEDRIVE - DISTRIBUICAO POR SCOPE ===')
    cur.execute('''
        SELECT scope, COUNT(*) as deals, 
               SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as won,
               SUM(CASE WHEN status = 'lost' THEN 1 ELSE 0 END) as lost,
               SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open
        FROM core.pd_deals 
        GROUP BY scope
    ''')
    print('{:<15} {:>10} {:>10} {:>10} {:>10}'.format('Scope', 'Total', 'Won', 'Lost', 'Open'))
    print('-' * 58)
    for row in cur.fetchall():
        print('{:<15} {:>10} {:>10} {:>10} {:>10}'.format(
            row[0], fmt_num(row[1]), fmt_num(row[2]), fmt_num(row[3]), fmt_num(row[4])
        ))
    
    # === ZENDESK STATUS ===
    print()
    print('=== ZENDESK - DISTRIBUICAO POR STATUS ===')
    cur.execute('''
        SELECT status, COUNT(*) 
        FROM core.zd_tickets 
        GROUP BY status 
        ORDER BY COUNT(*) DESC
    ''')
    print('{:<15} {:>10}'.format('Status', 'Tickets'))
    print('-' * 28)
    for row in cur.fetchall():
        print('{:<15} {:>10}'.format(row[0] or 'NULL', fmt_num(row[1])))
    
    conn.close()
    
    print()
    print('=' * 52)
    print('[OK] Health check concluido - {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print('=' * 52)
    
except Exception as e:
    print(f'[ERRO] {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"@

# Salvar e executar
$tempScript = Join-Path $ProjectRoot "scripts\_health_check_temp.py"
$pythonCode | Out-File -FilePath $tempScript -Encoding UTF8
python $tempScript
Remove-Item $tempScript -ErrorAction SilentlyContinue

Write-Host ""
