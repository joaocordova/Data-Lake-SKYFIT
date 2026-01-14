# -*- coding: utf-8 -*-
"""
Utilitários para operações no PostgreSQL.
"""
import json
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Iterator
import psycopg2
from psycopg2.extras import execute_values, Json


class DatabaseClient:
    """Cliente unificado para operações no PostgreSQL."""
    
    def __init__(self, host: str, port: int, database: str, 
                 user: str, password: str, sslmode: str = "require"):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.sslmode = sslmode
        self._conn = None
    
    @property
    def connection_params(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
            "sslmode": self.sslmode,
        }
    
    def connect(self):
        """Estabelece conexão com o banco."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self.connection_params)
        return self._conn
    
    def close(self):
        """Fecha a conexão."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None
    
    @contextmanager
    def cursor(self, commit: bool = True):
        """Context manager para cursor com auto-commit opcional."""
        conn = self.connect()
        cur = conn.cursor()
        try:
            yield cur
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    
    @contextmanager
    def transaction(self):
        """Context manager para transação explícita."""
        conn = self.connect()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    
    def execute(self, sql: str, params: Tuple = None) -> None:
        """Executa SQL sem retorno."""
        with self.cursor() as cur:
            cur.execute(sql, params)
    
    def execute_many(self, sql: str, params_list: List[Tuple]) -> int:
        """Executa SQL para múltiplos registros."""
        with self.cursor() as cur:
            cur.executemany(sql, params_list)
            return cur.rowcount
    
    def fetch_one(self, sql: str, params: Tuple = None) -> Optional[Tuple]:
        """Executa SQL e retorna uma linha."""
        with self.cursor(commit=False) as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    
    def fetch_all(self, sql: str, params: Tuple = None) -> List[Tuple]:
        """Executa SQL e retorna todas as linhas."""
        with self.cursor(commit=False) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    
    def fetch_scalar(self, sql: str, params: Tuple = None) -> Any:
        """Executa SQL e retorna valor escalar."""
        row = self.fetch_one(sql, params)
        return row[0] if row else None
    
    def table_exists(self, schema: str, table: str) -> bool:
        """Verifica se uma tabela existe."""
        sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """
        return self.fetch_scalar(sql, (schema, table))
    
    def get_row_count(self, schema: str, table: str) -> int:
        """Retorna contagem de linhas de uma tabela."""
        sql = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
        return self.fetch_scalar(sql) or 0
    
    def bulk_insert_jsonb(self, schema: str, table: str, 
                          records: List[Dict[str, Any]],
                          extra_columns: Dict[str, Any] = None,
                          batch_size: int = 1000) -> int:
        """
        Insere registros em massa em tabela com coluna JSONB.
        
        Args:
            schema: Nome do schema
            table: Nome da tabela
            records: Lista de dicts para inserir como JSONB
            extra_columns: Colunas adicionais com valores fixos
            batch_size: Tamanho do batch
        
        Returns:
            Número total de registros inseridos
        """
        if not records:
            return 0
        
        extra_columns = extra_columns or {}
        extra_cols = list(extra_columns.keys())
        extra_vals = list(extra_columns.values())
        
        # Monta a query
        cols = ["payload"] + extra_cols
        placeholders = ["%s"] * len(cols)
        
        sql = f"""
            INSERT INTO "{schema}"."{table}" ({", ".join(cols)})
            VALUES ({", ".join(placeholders)})
        """
        
        total = 0
        with self.cursor() as cur:
            batch = []
            for record in records:
                row = (Json(record),) + tuple(extra_vals)
                batch.append(row)
                
                if len(batch) >= batch_size:
                    cur.executemany(sql, batch)
                    total += len(batch)
                    batch = []
            
            if batch:
                cur.executemany(sql, batch)
                total += len(batch)
        
        return total
    
    def upsert_jsonb(self, schema: str, table: str,
                     records: List[Dict[str, Any]],
                     conflict_columns: List[str],
                     extra_columns: Dict[str, Any] = None,
                     batch_size: int = 1000) -> Tuple[int, int]:
        """
        Faz UPSERT de registros com JSONB.
        
        Args:
            schema: Nome do schema
            table: Nome da tabela
            records: Lista de dicts para inserir
            conflict_columns: Colunas para detectar conflito
            extra_columns: Colunas adicionais
            batch_size: Tamanho do batch
        
        Returns:
            Tuple (inserted, updated)
        """
        if not records:
            return 0, 0
        
        extra_columns = extra_columns or {}
        extra_cols = list(extra_columns.keys())
        extra_vals = list(extra_columns.values())
        
        cols = ["payload"] + extra_cols
        placeholders = ["%s"] * len(cols)
        
        # Colunas para update (exceto as de conflito)
        update_cols = [c for c in cols if c not in conflict_columns]
        update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        
        sql = f"""
            INSERT INTO "{schema}"."{table}" ({", ".join(cols)})
            VALUES ({", ".join(placeholders)})
            ON CONFLICT ({", ".join(conflict_columns)})
            DO UPDATE SET {update_set}
        """
        
        total = 0
        with self.cursor() as cur:
            batch = []
            for record in records:
                row = (Json(record),) + tuple(extra_vals)
                batch.append(row)
                
                if len(batch) >= batch_size:
                    cur.executemany(sql, batch)
                    total += len(batch)
                    batch = []
            
            if batch:
                cur.executemany(sql, batch)
                total += len(batch)
        
        return total, 0  # Postgres não diferencia facilmente insert/update


def create_db_client() -> DatabaseClient:
    """Factory para criar cliente do banco com configurações do ambiente."""
    import sys
    sys.path.insert(0, str(__file__).replace("/src/common/db.py", ""))
    from config.settings import postgres
    
    return DatabaseClient(
        host=postgres.HOST,
        port=postgres.PORT,
        database=postgres.DATABASE,
        user=postgres.USER,
        password=postgres.PASSWORD,
        sslmode=postgres.SSLMODE
    )
