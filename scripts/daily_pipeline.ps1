# ============================================================================
# SKYFIT DATA LAKE - PIPELINE DIÁRIO COMPLETO
# ============================================================================
# Executa o pipeline completo: Bronze → Silver → Gold → Health Check
# 
# Uso manual:
#   .\scripts\daily_pipeline.ps1
#
# Via Task Scheduler:
#   powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\skyfit-datalake\scripts\daily_pipeline.ps1"
# ============================================================================

param(
    [switch]$SkipBronze,
    [switch]$SkipSilver,
    [switch]$SkipGold,
    [switch]$SkipHealthCheck
)

# Configuração
$ErrorActionPreference = "Continue"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

# Timestamp para log
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogDir = Join-Path $ProjectRoot "logs"
$LogFile = Join-Path $LogDir "daily_pipeline_$Timestamp.log"

# Criar diretório de logs se não existir
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

# Função de logging
function Write-Log {
    param(
        [string]$Message,
        [ValidateSet("INFO", "SUCCESS", "WARNING", "ERROR")]
        [string]$Level = "INFO"
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "$timestamp | $Level | $Message"
    
    # Console com cores
    switch ($Level) {
        "SUCCESS" { Write-Host $logMessage -ForegroundColor Green }
        "WARNING" { Write-Host $logMessage -ForegroundColor Yellow }
        "ERROR"   { Write-Host $logMessage -ForegroundColor Red }
        default   { Write-Host $logMessage }
    }
    
    # Arquivo
    Add-Content -Path $LogFile -Value $logMessage
}

# Função para executar etapa
function Invoke-PipelineStep {
    param(
        [string]$Name,
        [string]$Command
    )
    
    Write-Log ">>> Iniciando: $Name"
    $startTime = Get-Date
    
    try {
        $output = Invoke-Expression $Command 2>&1
        $exitCode = $LASTEXITCODE
        
        # Log output
        $output | ForEach-Object { Add-Content -Path $LogFile -Value $_ }
        
        $duration = (Get-Date) - $startTime
        
        if ($exitCode -eq 0) {
            Write-Log "<<< Concluído: $Name ($('{0:N1}' -f $duration.TotalSeconds)s)" -Level "SUCCESS"
            return $true
        } else {
            Write-Log "!!! Falhou: $Name - Exit code: $exitCode" -Level "ERROR"
            return $false
        }
    }
    catch {
        Write-Log "!!! Falhou: $Name - $($_.Exception.Message)" -Level "ERROR"
        return $false
    }
}

# =============================================================================
# INÍCIO DO PIPELINE
# =============================================================================

$pipelineStart = Get-Date
$hasErrors = $false

Write-Log "============================================================"
Write-Log "SKYFIT DATA LAKE - PIPELINE DIÁRIO"
Write-Log "============================================================"
Write-Log "Projeto: $ProjectRoot"
Write-Log "Log: $LogFile"
Write-Log "============================================================"

# Ativar ambiente virtual
Write-Log "Ativando ambiente virtual..."
$venvActivate = Join-Path $ProjectRoot ".venv\Scripts\Activate.ps1"
if (Test-Path $venvActivate) {
    & $venvActivate
} else {
    Write-Log "Ambiente virtual não encontrado em $venvActivate" -Level "WARNING"
}

# =============================================================================
# CAMADA BRONZE (Extração das APIs → ADLS)
# =============================================================================

if (-not $SkipBronze) {
    Write-Log ""
    Write-Log "=== CAMADA BRONZE (Extração) ==="
    
    if (-not (Invoke-PipelineStep "Pipedrive Bronze" "python src/extractors/pipedrive_bronze.py")) {
        $hasErrors = $true
    }
    
    if (-not (Invoke-PipelineStep "Zendesk Bronze" "python src/extractors/zendesk_bronze.py")) {
        $hasErrors = $true
    }
} else {
    Write-Log ""
    Write-Log "=== CAMADA BRONZE (Pulada) ===" -Level "WARNING"
}

# =============================================================================
# CAMADA SILVER (ADLS → PostgreSQL STG)
# =============================================================================

if (-not $SkipSilver) {
    Write-Log ""
    Write-Log "=== CAMADA SILVER (STG) ==="
    
    if (-not (Invoke-PipelineStep "Pipedrive STG" "python src/loaders/load_pipedrive_stg.py")) {
        $hasErrors = $true
    }
    
    if (-not (Invoke-PipelineStep "Zendesk STG" "python src/loaders/load_zendesk_stg.py")) {
        $hasErrors = $true
    }
} else {
    Write-Log ""
    Write-Log "=== CAMADA SILVER (Pulada) ===" -Level "WARNING"
}

# =============================================================================
# CAMADA GOLD (STG → PostgreSQL CORE)
# =============================================================================

if (-not $SkipGold) {
    Write-Log ""
    Write-Log "=== CAMADA GOLD (CORE) ==="
    
    if (-not (Invoke-PipelineStep "Pipedrive CORE" "python src/transformers/normalize_pipedrive.py")) {
        $hasErrors = $true
    }
    
    if (-not (Invoke-PipelineStep "Zendesk CORE" "python src/transformers/normalize_zendesk.py")) {
        $hasErrors = $true
    }
} else {
    Write-Log ""
    Write-Log "=== CAMADA GOLD (Pulada) ===" -Level "WARNING"
}

# =============================================================================
# HEALTH CHECK (Validação)
# =============================================================================

if (-not $SkipHealthCheck) {
    Write-Log ""
    Write-Log "=== HEALTH CHECK ==="
    
    $healthCheckScript = Join-Path $ProjectRoot "scripts\health_check.ps1"
    if (Test-Path $healthCheckScript) {
        & $healthCheckScript 2>&1 | ForEach-Object { 
            Write-Host $_
            Add-Content -Path $LogFile -Value $_
        }
        Write-Log "Health check concluído" -Level "SUCCESS"
    } else {
        Write-Log "Script health_check.ps1 não encontrado" -Level "WARNING"
    }
} else {
    Write-Log ""
    Write-Log "=== HEALTH CHECK (Pulado) ===" -Level "WARNING"
}

# =============================================================================
# RESUMO FINAL
# =============================================================================

$pipelineDuration = (Get-Date) - $pipelineStart

Write-Log ""
Write-Log "============================================================"

if ($hasErrors) {
    Write-Log "PIPELINE CONCLUÍDO COM ERROS" -Level "WARNING"
} else {
    Write-Log "PIPELINE CONCLUÍDO COM SUCESSO" -Level "SUCCESS"
}

Write-Log "Verifique o log: $LogFile"
Write-Log "Duração total: $('{0:N2}' -f $pipelineDuration.TotalMinutes) minutos"
Write-Log "============================================================"

# Exit code
if ($hasErrors) {
    exit 1
} else {
    exit 0
}
