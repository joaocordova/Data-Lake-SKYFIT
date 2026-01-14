# -*- coding: utf-8 -*-
"""
Configuração de logging padronizada para o projeto.
"""
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


def setup_logger(
    name: str,
    log_dir: Optional[Path] = None,
    level: str = "INFO",
    log_to_file: bool = True,
    log_to_console: bool = True
) -> logging.Logger:
    """
    Configura e retorna um logger padronizado.
    
    Args:
        name: Nome do logger (geralmente __name__ ou nome do script)
        log_dir: Diretório para salvar logs
        level: Nível de log (DEBUG, INFO, WARNING, ERROR)
        log_to_file: Se deve salvar em arquivo
        log_to_console: Se deve imprimir no console
    
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remove handlers existentes para evitar duplicação
    logger.handlers.clear()
    
    # Formato padrão
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_to_file and log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Nome do arquivo com data
        today = datetime.now().strftime("%Y-%m-%d")
        log_file = log_dir / f"{name}_{today}.log"
        
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


class RunLogger:
    """
    Logger especializado para runs de pipeline.
    Mantém métricas e gera relatório final.
    """
    
    def __init__(self, run_name: str, log_dir: Optional[Path] = None):
        self.run_name = run_name
        self.start_time = datetime.now()
        self.metrics = {}
        self.errors = []
        self.warnings = []
        
        self.logger = setup_logger(
            name=run_name,
            log_dir=log_dir,
            level="INFO"
        )
    
    def info(self, message: str):
        self.logger.info(message)
    
    def warning(self, message: str):
        self.warnings.append(message)
        self.logger.warning(message)
    
    def error(self, message: str):
        self.errors.append(message)
        self.logger.error(message)
    
    def debug(self, message: str):
        self.logger.debug(message)
    
    def set_metric(self, key: str, value):
        """Define uma métrica para o relatório final."""
        self.metrics[key] = value
    
    def increment_metric(self, key: str, value: int = 1):
        """Incrementa uma métrica numérica."""
        self.metrics[key] = self.metrics.get(key, 0) + value
    
    def get_duration_seconds(self) -> float:
        """Retorna duração do run em segundos."""
        return (datetime.now() - self.start_time).total_seconds()
    
    def get_summary(self) -> dict:
        """Retorna resumo do run."""
        return {
            "run_name": self.run_name,
            "start_time": self.start_time.isoformat(),
            "end_time": datetime.now().isoformat(),
            "duration_seconds": round(self.get_duration_seconds(), 2),
            "metrics": self.metrics,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": self.errors,
            "warnings": self.warnings,
            "status": "FAILED" if self.errors else "SUCCESS"
        }
    
    def log_summary(self):
        """Loga o resumo do run."""
        summary = self.get_summary()
        
        self.logger.info("=" * 60)
        self.logger.info(f"RUN SUMMARY: {summary['run_name']}")
        self.logger.info(f"Status: {summary['status']}")
        self.logger.info(f"Duration: {summary['duration_seconds']}s")
        
        if self.metrics:
            self.logger.info("Metrics:")
            for key, value in self.metrics.items():
                self.logger.info(f"  {key}: {value}")
        
        if self.warnings:
            self.logger.info(f"Warnings ({len(self.warnings)}):")
            for w in self.warnings[:5]:  # Mostra até 5
                self.logger.info(f"  - {w}")
        
        if self.errors:
            self.logger.info(f"Errors ({len(self.errors)}):")
            for e in self.errors[:5]:  # Mostra até 5
                self.logger.info(f"  - {e}")
        
        self.logger.info("=" * 60)
        
        return summary
