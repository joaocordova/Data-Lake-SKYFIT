# ============================================================================
# SKYFIT DATA LAKE - SETUP TASK SCHEDULER
# ============================================================================
# Configura o Windows Task Scheduler para executar o pipeline diário
# Uso: .\scripts\setup_task_scheduler.ps1
# ============================================================================

param(
    [string]$TaskTime = "06:00",
    [string]$ProjectPath = "C:\skyfit-datalake"
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "SKYFIT DATA LAKE - CONFIGURAÇÃO DO AGENDADOR" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Verificar se o projeto existe
if (-not (Test-Path $ProjectPath)) {
    Write-Host "[ERRO] Diretório do projeto não encontrado: $ProjectPath" -ForegroundColor Red
    exit 1
}

# Nome da tarefa
$TaskName = "SkyFit - Daily Pipeline"

# Script a ser executado
$ScriptPath = Join-Path $ProjectPath "scripts\daily_pipeline.ps1"

if (-not (Test-Path $ScriptPath)) {
    Write-Host "[ERRO] Script não encontrado: $ScriptPath" -ForegroundColor Red
    exit 1
}

# Comando PowerShell
$Action = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""

Write-Host "[INFO] Configurações:" -ForegroundColor Yellow
Write-Host "       Tarefa: $TaskName"
Write-Host "       Horário: $TaskTime (diariamente)"
Write-Host "       Script: $ScriptPath"
Write-Host ""

# Remover tarefa existente (se houver)
$existingTask = schtasks.exe /Query /TN $TaskName 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "[INFO] Removendo tarefa existente..." -ForegroundColor Yellow
    schtasks.exe /Delete /TN $TaskName /F | Out-Null
}

# Criar nova tarefa
Write-Host "[INFO] Criando tarefa agendada..." -ForegroundColor Yellow

$result = schtasks.exe /Create /F `
    /TN $TaskName `
    /SC DAILY `
    /ST $TaskTime `
    /TR $Action `
    /RL HIGHEST `
    /NP

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "[SUCESSO] Tarefa criada com sucesso!" -ForegroundColor Green
    Write-Host ""
    
    # Mostrar detalhes da tarefa
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "DETALHES DA TAREFA" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
    schtasks.exe /Query /TN $TaskName /V /FO LIST | Select-String -Pattern "Nome da tarefa|Hora da próxima|Status|Tarefa a ser executada"
    
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "COMANDOS ÚTEIS" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Executar agora:"
    Write-Host "  schtasks.exe /Run /TN `"$TaskName`"" -ForegroundColor White
    Write-Host ""
    Write-Host "Ver status:"
    Write-Host "  schtasks.exe /Query /TN `"$TaskName`" /V /FO LIST" -ForegroundColor White
    Write-Host ""
    Write-Host "Desativar:"
    Write-Host "  schtasks.exe /Change /TN `"$TaskName`" /DISABLE" -ForegroundColor White
    Write-Host ""
    Write-Host "Remover:"
    Write-Host "  schtasks.exe /Delete /TN `"$TaskName`" /F" -ForegroundColor White
    Write-Host ""
    Write-Host "Ver último log:"
    Write-Host "  Get-Content (Get-ChildItem `"$ProjectPath\logs\daily_pipeline_*.log`" | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName -Tail 50" -ForegroundColor White
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "[ERRO] Falha ao criar tarefa. Verifique permissões de administrador." -ForegroundColor Red
    exit 1
}
