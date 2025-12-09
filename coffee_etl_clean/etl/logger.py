import logging
import json
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from .config import settings

LOGGER_NAME = "coffee_etl"


class JsonFormatter(logging.Formatter):
    def format(self, record):
        data = {
            "time": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
        }
        
        # Thêm message nếu có
        msg = record.getMessage()
        if msg:
            data["message"] = msg

        # Thêm extra_data (nhưng bỏ 'raw' nếu có)
        if hasattr(record, "extra_data"):
            for k, v in record.extra_data.items():
                if k != "raw":  # không log raw
                    data[k] = v

        return json.dumps(data, ensure_ascii=False)


def get_logger():
    logger = logging.getLogger(LOGGER_NAME)
    if logger.handlers:
        return logger

    logs_dir = Path(settings.LOG_DIR)
    logs_dir.mkdir(exist_ok=True)

    logger.setLevel(logging.INFO)
    logger.propagate = False

    # ===== Console handler (chỉ dùng khi debug) =====
    console = logging.StreamHandler()
    console.setFormatter(JsonFormatter())
    logger.addHandler(console)

    # ===== pipeline.log =====
    pipeline_handler = TimedRotatingFileHandler(
        logs_dir / "pipeline.log", when="midnight", backupCount=7, encoding="utf-8"
    )
    pipeline_handler.setFormatter(JsonFormatter())
    logger.addHandler(pipeline_handler)

    # ===== data.log =====
    data_handler = RotatingFileHandler(
        logs_dir / "data.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    data_handler.setFormatter(JsonFormatter())
    logger.addHandler(data_handler)

    # ===== error.log =====
    error_handler = RotatingFileHandler(
        logs_dir / "error.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(JsonFormatter())
    logger.addHandler(error_handler)

    return logger


logger = get_logger()
