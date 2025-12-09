from dotenv import load_dotenv
from pathlib import Path
import os

# Lấy đường dẫn tới thư mục gốc project
BASE_DIR = Path(__file__).resolve().parent.parent

# Load file .env trong thư mục gốc
load_dotenv(BASE_DIR / ".env")


class Settings:
    # App settings
    APP_ENV = os.getenv("APP_ENV", "development")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR: str = os.getenv("LOG_DIR", "logs")
    
    # RabbitMQ settings
    RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
    RABBITMQ_MANAGEMENT_PORT = int(os.getenv("RABBITMQ_MANAGEMENT_PORT", "15672"))
    RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
    
    # Source Database: ComVanPhong (Data Source 2 - nguồn nội bộ)
    SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST", "localhost")
    SOURCE_DB_PORT = int(os.getenv("SOURCE_DB_PORT", "1433"))
    SOURCE_DB_NAME = os.getenv("SOURCE_DB_NAME", "ComVanPhong")
    SOURCE_DB_TRUSTED_CONNECTION = os.getenv("SOURCE_DB_TRUSTED_CONNECTION", "true").lower() == "true"
    SOURCE_DB_DRIVER = os.getenv("SOURCE_DB_DRIVER", "ODBC Driver 17 for SQL Server")
    
    # Target Database: newdata (Đích cuối cùng - chứa staging và DWH)
    TARGET_DB_HOST = os.getenv("TARGET_DB_HOST", "localhost")
    TARGET_DB_PORT = int(os.getenv("TARGET_DB_PORT", "1433"))
    TARGET_DB_NAME = os.getenv("TARGET_DB_NAME", "newdata")
    TARGET_DB_TRUSTED_CONNECTION = os.getenv("TARGET_DB_TRUSTED_CONNECTION", "true").lower() == "true"
    TARGET_DB_DRIVER = os.getenv("TARGET_DB_DRIVER", "ODBC Driver 17 for SQL Server")


settings = Settings()
