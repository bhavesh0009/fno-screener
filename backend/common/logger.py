"""
Structured logging setup for the data pipeline.
Provides JSON-formatted logs with timestamps and context.
"""
import logging
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any, Dict

from .config import LOGS_DIR


class JSONFormatter(logging.Formatter):
    """Custom formatter that outputs logs in JSON format."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add extra context if provided
        if hasattr(record, "context") and record.context:
            log_data["context"] = record.context
            
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
            
        return json.dumps(log_data)


class PipelineLogger:
    """
    Structured logger for the data pipeline.
    
    Usage:
        from common.logger import get_pipeline_logger
        logger = get_pipeline_logger("collector")
        logger.info("Fetching data", symbol="SBIN", source="nselib")
    """
    
    def __init__(self, name: str, log_level: int = logging.INFO):
        self.logger = logging.getLogger(f"pipeline.{name}")
        self.logger.setLevel(log_level)
        
        # Avoid duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()
    
    def _setup_handlers(self):
        """Set up file and console handlers."""
        # Ensure logs directory exists
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        
        # File handler with JSON format
        log_file = LOGS_DIR / f"pipeline_{datetime.now().strftime('%Y-%m-%d')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(JSONFormatter())
        self.logger.addHandler(file_handler)
        
        # Console handler with simple format (context will be appended to message)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        self.logger.addHandler(console_handler)
    
    def _log(self, level: int, message: str, **context):
        """Log with optional context."""
        extra = {"context": context} if context else {}
        # Append context to message for console readability
        if context:
            context_str = " | " + ", ".join(f"{k}={v}" for k, v in context.items())
            message = message + context_str
        self.logger.log(level, message, extra=extra)
    
    def debug(self, message: str, **context):
        self._log(logging.DEBUG, message, **context)
    
    def info(self, message: str, **context):
        self._log(logging.INFO, message, **context)
    
    def warning(self, message: str, **context):
        self._log(logging.WARNING, message, **context)
    
    def error(self, message: str, **context):
        self._log(logging.ERROR, message, **context)
    
    def exception(self, message: str, **context):
        """Log exception with traceback."""
        extra = {"context": context} if context else {}
        self.logger.exception(message, extra=extra)


# Module-level logger cache
_loggers: Dict[str, PipelineLogger] = {}


def get_pipeline_logger(name: str = "main") -> PipelineLogger:
    """
    Get or create a pipeline logger.
    
    Args:
        name: Logger name (e.g., 'collector', 'processor', 'main')
    
    Returns:
        PipelineLogger instance
    """
    if name not in _loggers:
        _loggers[name] = PipelineLogger(name)
    return _loggers[name]
