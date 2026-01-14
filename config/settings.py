# -*- coding: utf-8 -*-
"""
Configurações centralizadas do projeto Skyfit Data Lake.
Carrega variáveis de ambiente e fornece defaults seguros.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Determina o diretório raiz do projeto
PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
LOGS_DIR = PROJECT_ROOT / "logs"

# Carrega .env se existir
env_file = CONFIG_DIR / ".env"
if env_file.exists():
    load_dotenv(env_file)


def get_env(name: str, default: str = None, required: bool = False) -> str:
    """Obtém variável de ambiente com validação."""
    value = os.environ.get(name, "").strip()
    if not value:
        if required:
            raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
        return default
    return value


# =============================================================================
# AZURE STORAGE
# =============================================================================
class AzureStorageConfig:
    ACCOUNT = get_env("AZURE_STORAGE_ACCOUNT", required=True)
    KEY = get_env("AZURE_STORAGE_KEY", required=True)
    CONTAINER = get_env("ADLS_CONTAINER", "datalake")
    
    @property
    def account_url(self) -> str:
        return f"https://{self.ACCOUNT}.blob.core.windows.net"


# =============================================================================
# POSTGRESQL
# =============================================================================
class PostgresConfig:
    HOST = get_env("PG_HOST", required=True)
    PORT = int(get_env("PG_PORT", "5432"))
    DATABASE = get_env("PG_DATABASE", "postgres")
    USER = get_env("PG_USER", required=True)
    PASSWORD = get_env("PG_PASSWORD", required=True)
    SSLMODE = get_env("PG_SSLMODE", "require")
    
    @property
    def connection_string(self) -> str:
        return (
            f"host={self.HOST} port={self.PORT} dbname={self.DATABASE} "
            f"user={self.USER} password={self.PASSWORD} sslmode={self.SSLMODE}"
        )
    
    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}"
            f"/{self.DATABASE}?sslmode={self.SSLMODE}"
        )


# =============================================================================
# PIPEDRIVE
# =============================================================================
class PipedriveConfig:
    COMPANY_DOMAIN = get_env("PIPEDRIVE_COMPANY_DOMAIN")
    TOKEN_COMERCIAL = get_env("PIPEDRIVE_TOKEN_COMERCIAL")
    TOKEN_EXPANSAO = get_env("PIPEDRIVE_TOKEN_EXPANSAO")
    
    SCOPES = ["comercial", "expansao"]
    ENTITIES = ["deals", "persons", "organizations", "activities", "pipelines", "stages", "users"]
    
    @classmethod
    def get_token(cls, scope: str) -> str:
        tokens = {
            "comercial": cls.TOKEN_COMERCIAL,
            "expansao": cls.TOKEN_EXPANSAO,
        }
        return tokens.get(scope)


# =============================================================================
# ZENDESK
# =============================================================================
class ZendeskConfig:
    SUBDOMAIN = get_env("ZENDESK_SUBDOMAIN")
    EMAIL = get_env("ZENDESK_EMAIL")
    API_TOKEN = get_env("ZENDESK_API_TOKEN")
    
    SCOPES = ["support"]
    ENTITIES = ["tickets", "users", "organizations", "groups", "ticket_fields", "ticket_forms"]


# =============================================================================
# LOGGING
# =============================================================================
class LogConfig:
    LEVEL = get_env("LOG_LEVEL", "INFO")
    DIR = LOGS_DIR
    
    @classmethod
    def ensure_dir(cls):
        cls.DIR.mkdir(parents=True, exist_ok=True)


# Instâncias singleton para uso direto
azure_storage = AzureStorageConfig()
postgres = PostgresConfig()
pipedrive = PipedriveConfig()
zendesk = ZendeskConfig()
log_config = LogConfig()
