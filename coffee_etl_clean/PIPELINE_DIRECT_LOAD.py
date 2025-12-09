"""
PIPELINE DIRECT LOAD
====================
Pipeline hoàn chỉnh: Producer → Consumer → Validate → Transform → Load (trực tiếp vào SQL)

Giống main.py nhưng với cấu trúc rõ ràng hơn:
- STEP 1: Producer → RabbitMQ
- STEP 2: Consumer → RAW files (staging/raw/)
- STEP 3: Validate → CLEAN/ERROR files (staging/clean/, staging/error/)
- run
"""

import time
import jso
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set

from etl.broker.rabbitmq_client import RabbitMQClient
from etl.db.database_factory import DatabaseFactory, SourceDBReader
from etl.db.sql_client import SQLServerClient
from etl.readers.csv_staging_reader import csv_staging_reader
from etl.quality.rule_registry import rule_registry
from etl.transformers.data_transformer import DataTransformer
from etl.config import settings
from etl.logger import logger


class DatabaseManager:
    """Quản lý việc tạo database + schema + staging tables."""
    
    @staticmethod
    def create_database(db_name: str, sql_client):
        """Tạo database mới nếu chưa tồn tại."""
        try:
            check_query = f"SELECT database_id FROM sys.databases WHERE name = '{db_name}'"
            result = sql_client.execute_query(check_query)
            
            if result:
                logger.info("   Database '%s' đã tồn tại", db_name)
                return True
            
            create_query = f"CREATE DATABASE [{db_name}]"
            sql_client.connection.autocommit = True
            sql_client.cursor.execute(create_query)
            sql_client.connection.autocommit = False
            
            logger.info("   ✅ Đã tạo database: %s", db_name)
            return True
            
        except Exception as e:
            logger.error("   ❌ Lỗi tạo database: %s", e)
            return False
    
    @staticmethod
    def create_staging_schema(sql_client):
        """Tạo schema staging."""
        try:
            schema_query = """
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
            BEGIN
                EXEC('CREATE SCHEMA staging')
            END
            """
            sql_client.execute_non_query(schema_query)
            logger.info("   ✅ Đã tạo schema: staging")
            return True
        except Exception as e:
            logger.error("   ❌ Lỗi tạo schema: %s", e)
            return False
    
    @staticmethod
    def create_staging_tables(sql_client):
        """Tạo các staging tables."""
        # Tạo từng table riêng lẻ (không dùng GO statements)
        tables = [
            # Khách hàng CSV
            """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'khach_hang_csv' AND schema_id = SCHEMA_ID('staging'))
            CREATE TABLE staging.khach_hang_csv (
                id INT IDENTITY(1,1) PRIMARY KEY,
                customer_id NVARCHAR(50),
                ho_ten NVARCHAR(200),
                sdt NVARCHAR(20),
                thanh_pho NVARCHAR(100),
                email NVARCHAR(200),
                extract_time DATETIME,
                loaded_at DATETIME DEFAULT GETDATE()
            )
            """,
            # Khách hàng SQL
            """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'khach_hang_sql' AND schema_id = SCHEMA_ID('staging'))
            CREATE TABLE staging.khach_hang_sql (
                id INT IDENTITY(1,1) PRIMARY KEY,
                customer_id NVARCHAR(50),
                ho_ten NVARCHAR(200),
                sdt NVARCHAR(20),
                thanh_pho NVARCHAR(100),
                email NVARCHAR(200),
                extract_time DATETIME,
                loaded_at DATETIME DEFAULT GETDATE()
            )
            """,
            # Loại món CSV
            """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'loai_mon_csv' AND schema_id = SCHEMA_ID('staging'))
            CREATE TABLE staging.loai_mon_csv (
                id INT IDENTITY(1,1) PRIMARY KEY,
                ma_loai NVARCHAR(50),
                ten_loai NVARCHAR(200),
                mo_ta NVARCHAR(500),
                extract_time DATETIME,
                loaded_at DATETIME DEFAULT GETDATE()
            )
            """,
            # Loại món SQL
            """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'loai_mon_sql' AND schema_id = SCHEMA_ID('staging'))
            CREATE TABLE staging.loai_mon_sql (
                id INT IDENTITY(1,1) PRIMARY KEY,
                ma_loai NVARCHAR(50),
                ten_loai NVARCHAR(200),
                mo_ta NVARCHAR(500),
                extract_time DATETIME,
                loaded_at DATETIME DEFAULT GETDATE()
            )
            """,
            # Món CSV
            """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'mon_csv' AND schema_id = SCHEMA_ID('staging'))
            CREATE TABLE staging.mon_csv (
                id INT IDENTITY(1,1) PRIMARY KEY,
                ten_mon NVARCHAR(200),
                loai_id INT,
                gia DECIMAL(18,2),
                extract_time DATETIME,
                loaded_at DATETIME DEFAULT GETDATE()
            )
            """,
            # Món SQL
            """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'mon_sql' AND schema_id = SCHEMA_ID('staging'))
            CREATE TABLE staging.mon_sql (
                id INT IDENTITY(1,1) PRIMARY KEY,
                ten_mon NVARCHAR(200),
                loai_id INT,
                gia DECIMAL(18,2),
                extract_time DATETIME,
                loaded_at DATETIME DEFAULT GETDATE()
            )
            """,
            # Nguyên liệu CSV
            """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'nguyen_lieu_csv' AND schema_id = SCHEMA_ID('staging'))
            CREATE TABLE staging.nguyen_lieu_csv (
                id INT IDENTITY(1,1) PRIMARY KEY,
                ma_nguyen_lieu NVARCHAR(50),
                ten_nguyen_lieu NVARCHAR(200),
                so_luong DECIMAL(18,2),
                don_vi NVARCHAR(50),
                gia DECIMAL(18,2),
                nha_cung_cap NVARCHAR(200),
                ngay_nhap DATE,
                extract_time DATETIME,
                loaded_at DATETIME DEFAULT GETDATE()
            )
            """,
            # Nguyên liệu SQL
            """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'nguyen_lieu_sql' AND schema_id = SCHEMA_ID('staging'))
            CREATE TABLE staging.nguyen_lieu_sql (
                id INT IDENTITY(1,1) PRIMARY KEY,
                ma_nguyen_lieu NVARCHAR(50),
                ten_nguyen_lieu NVARCHAR(200),
                so_luong DECIMAL(18,2),
                don_vi NVARCHAR(50),
                gia DECIMAL(18,2),
                nha_cung_cap NVARCHAR(200),
                ngay_nhap DATE,
                extract_time DATETIME,
                loaded_at DATETIME DEFAULT GETDATE()
            )
            """,
            # Đặt hàng CSV
            """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dat_hang_csv' AND schema_id = SCHEMA_ID('staging'))
            CREATE TABLE staging.dat_hang_csv (
                id INT IDENTITY(1,1) PRIMARY KEY,
                khach_hang_id NVARCHAR(50),
                mon_id NVARCHAR(50),
                so_luong INT,
                ngay_dat DATE,
                trang_thai NVARCHAR(50),
                extract_time DATETIME,
                loaded_at DATETIME DEFAULT GETDATE()
            )
            """,
            # Đặt hàng SQL
            """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dat_hang_sql' AND schema_id = SCHEMA_ID('staging'))
            CREATE TABLE staging.dat_hang_sql (
                id INT IDENTITY(1,1) PRIMARY KEY,
                khach_hang_id NVARCHAR(50),
                mon_id NVARCHAR(50),
                so_luong INT,
                ngay_dat DATE,
                trang_thai NVARCHAR(50),
                extract_time DATETIME,
                loaded_at DATETIME DEFAULT GETDATE()
            )
            """
        ]
        
        try:
            for table_sql in tables:
                sql_client.execute_non_query(table_sql)
            logger.info("   ✅ Đã tạo staging tables")
            return True
        except Exception as e:
            logger.error("   ❌ Lỗi tạo tables: %s", e)
            return False


class DirectLoadPipeline:
    """Pipeline với direct load - không đọc lại file CSV."""
    
    def __init__(self):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.db_name = f"DB_{self.run_id}"
        self.target_db = None
        
        # Thư mục
        self.raw_dir = Path("staging") / "raw"
        self.clean_dir = Path("staging") / "clean"
        self.error_dir = Path("staging") / "error"
        
        for dir_path in [self.raw_dir, self.clean_dir, self.error_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        self.stats = {
            "produced": {},
            "consumed": {},
            "valid": {},
            "invalid": {},
            "loaded": {}
        }
    
    def run(self):
        logger.info("=" * 80)
        logger.info("PIPELINE DIRECT LOAD")
        logger.info("Run ID: %s", self.run_id)
        logger.info("Database: %s", self.db_name)
        logger.info("=" * 80)
        
        try:
            # STEP 0: Setup Database
            logger.info("\n🔹 STEP 0: Setup Database")
            logger.info("-" * 80)
            self.setup_database()
            
            # STEP 1: Producer
            logger.info("\n🔹 STEP 1: Producer → RabbitMQ")
            logger.info("-" * 80)
            self.producer_phase()
            
            time.sleep(2)  # Đợi messages được gửi xong
            
            # STEP 2-4: Consumer → Validate → Transform → Load (trực tiếp)
            logger.info("\n🔹 STEP 2-4: Consumer → Validate → Transform → Load")
            logger.info("-" * 80)
            self.consumer_validate_transform_load()
            
            self.print_summary()
            
        except Exception as e:
            logger.error("❌ Lỗi pipeline: %s", e, exc_info=True)
            raise
        finally:
            if self.target_db:
                self.target_db.close()
    
    def setup_database(self):
        """Setup database và staging tables."""
        master_db = SQLServerClient(
            server=f"{settings.TARGET_DB_HOST},{settings.TARGET_DB_PORT}",
            database="master",
            driver=settings.TARGET_DB_DRIVER,
            trusted_connection=settings.TARGET_DB_TRUSTED_CONNECTION,
        )
        
        try:
            master_db.connect()
            logger.info("📦 Tạo database: %s", self.db_name)
            
            if DatabaseManager.create_database(self.db_name, master_db):
                logger.info("✅ Database đã sẵn sàng")
            else:
                raise Exception("Không thể tạo database")
        finally:
            master_db.close()
        
        # Kết nối database mới
        self.target_db = SQLServerClient(
            server=f"{settings.TARGET_DB_HOST},{settings.TARGET_DB_PORT}",
            database=self.db_name,
            driver=settings.TARGET_DB_DRIVER,
            trusted_connection=settings.TARGET_DB_TRUSTED_CONNECTION,
        )
        
        self.target_db.connect()
        DatabaseManager.create_staging_schema(self.target_db)
        DatabaseManager.create_staging_tables(self.target_db)
        logger.info("✅ Setup database hoàn thành")
    
    def producer_phase(self):
        """Producer phase - giống STEP1."""
        with RabbitMQClient(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            username=settings.RABBITMQ_USER,
            password=settings.RABBITMQ_PASSWORD,
        ) as rabbitmq:
            
            logger.info("📄 Producer 1: CSV files...")
            csv_stats = self.produce_from_csv(rabbitmq)
            self.stats["produced"].update(csv_stats)
            
            logger.info("\n💾 Producer 2: SQL Server...")
            sql_stats = self.produce_from_sql(rabbitmq)
            self.stats["produced"].update(sql_stats)
            
            logger.info("\n✅ Producer phase hoàn thành!")
            logger.info("   Tổng: %s messages", sum(self.stats["produced"].values()))
    
    def produce_from_csv(self, rabbitmq: RabbitMQClient) -> Dict[str, int]:
        """Producer từ CSV files."""
        stats = {}
        script_dir = Path(__file__).parent
        data_dir = script_dir / "data"
        
        csv_files = {
            "khachhang.csv": "queue_khach_hang",
            "loaisanpham.csv": "queue_loai_mon",
            "tensanpham.csv": "queue_mon",
            "nguyenlieu.csv": "queue_nguyen_lieu",
            "dathang.csv": "queue_dat_hang",
        }
        
        for file_name, queue_name in csv_files.items():
            file_path = data_dir / file_name
            
            if not file_path.exists():
                logger.warning("   ⚠️  Không tìm thấy: %s", file_name)
                continue
            
            rabbitmq.declare_queue(queue_name, durable=True)
            count = 0
            
            try:
                for row in csv_staging_reader(str(file_path)):
                    message = {
                        "source": "csv",
                        "entity_type": queue_name.replace("queue_", ""),
                        "data": row,
                        "metadata": {
                            "file": file_name,
                            "extract_time": datetime.now().isoformat(),
                        },
                    }
                    rabbitmq.publish(queue_name, message, persistent=True)
                    count += 1
                
                stats[f"csv_{queue_name}"] = count
                logger.info("   ✓ %s: %s messages → %s", file_name, count, queue_name)
                
            except Exception as e:
                logger.error("   ✗ Lỗi %s: %s", file_name, e)
        
        return stats
    
    def produce_from_sql(self, rabbitmq: RabbitMQClient) -> Dict[str, int]:
        """Producer từ SQL Server."""
        stats = {}
        source_db = DatabaseFactory.create_source_db()
        
        try:
            source_db.connect()
            reader = SourceDBReader(source_db)
            tables = reader.get_all_tables(schema="dbo")
            
            for table in tables:
                if table.lower() in ["sysdiagrams"]:
                    continue
                
                entity_type = self.infer_entity_type(table)
                queue_name = f"queue_{entity_type}"
                
                rabbitmq.declare_queue(queue_name, durable=True)
                
                try:
                    data = reader.read_table(table, schema="dbo")
                    
                    from etl.utils.json_encoder import convert_sql_row_to_json_compatible
                    
                    for row in data:
                        json_row = convert_sql_row_to_json_compatible(row)
                        message = {
                            "source": "sql",
                            "entity_type": entity_type,
                            "data": json_row,
                            "metadata": {
                                "table": table,
                                "database": settings.SOURCE_DB_NAME,
                                "extract_time": datetime.now().isoformat(),
                            },
                        }
                        rabbitmq.publish(queue_name, message, persistent=True)
                    
                    stats[f"sql_{queue_name}"] = len(data)
                    logger.info("   ✓ %s: %s messages → %s", table, len(data), queue_name)
                    
                except Exception as e:
                    logger.error("   ✗ Lỗi %s: %s", table, e)
        
        finally:
            source_db.close()
        
        return stats
    
    def consumer_validate_transform_load(self):
        """Consumer → Validate → Transform → Load (trực tiếp, không ghi file)."""
        queues = [
            ("queue_khach_hang", "khach_hang"),
            ("queue_loai_mon", "loai_mon"),
            ("queue_mon", "mon"),
            ("queue_nguyen_lieu", "nguyen_lieu"),
            ("queue_dat_hang", "dat_hang"),
        ]
        
        for queue_name, entity_type in queues:
            logger.info("\n📥 Processing: %s", queue_name)
            self.process_queue(queue_name, entity_type)
        
        logger.info("\n✅ Pipeline hoàn thành!")
    
    def process_queue(self, queue_name: str, entity_type: str):
        """Xử lý một queue: consume → validate → transform → load."""
        with RabbitMQClient(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            username=settings.RABBITMQ_USER,
            password=settings.RABBITMQ_PASSWORD,
        ) as rabbitmq:
            
            try:
                method_frame = rabbitmq.channel.queue_declare(
                    queue=queue_name, durable=True, passive=True
                )
                message_count = method_frame.method.message_count
            except Exception:
                logger.warning("   Queue không tồn tại: %s", queue_name)
                return
            
            if message_count == 0:
                logger.info("   Queue rỗng")
                return
            
            logger.info("   Messages: %s", message_count)
            
            consumed = 0
            csv_valid_rows: List[Dict] = []
            sql_valid_rows: List[Dict] = []
            csv_invalid_rows: List[Dict] = []
            sql_invalid_rows: List[Dict] = []
            
            seen_ids = set()
            seen_emails = set()
            
            def callback(ch, method, properties, body):
                nonlocal consumed
                
                try:
                    message = json.loads(body.decode("utf-8"))
                    data = message.get("data", {})
                    source = message.get("source", "unknown")
                    
                    # Validate
                    is_valid, fixed_row, errors = rule_registry.validate_row(
                        entity_type=entity_type,
                        row=data,
                        context={
                            "existing_ids": seen_ids,
                            "existing_emails": seen_emails,
                            "source": source,
                        },
                    )
                    
                    if is_valid:
                        if source == "csv":
                            csv_valid_rows.append(fixed_row)
                        else:
                            sql_valid_rows.append(fixed_row)
                        
                        # Track IDs
                        for id_field in ["id", "customer_id"]:
                            if id_field in fixed_row and fixed_row[id_field]:
                                try:
                                    seen_ids.add(int(fixed_row[id_field]))
                                except (ValueError, TypeError):
                                    pass
                        
                        if "email" in fixed_row and fixed_row["email"]:
                            seen_emails.add(fixed_row["email"].lower())
                    else:
                        if source == "csv":
                            csv_invalid_rows.append({"data": data, "errors": errors})
                        else:
                            sql_invalid_rows.append({"data": data, "errors": errors})
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    consumed += 1
                    
                except Exception as e:
                    logger.error("   Lỗi xử lý message: %s", e)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            rabbitmq.channel.basic_qos(prefetch_count=1)
            rabbitmq.channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )
            
            while consumed < message_count:
                rabbitmq.connection.process_data_events(time_limit=1)
            
            total_valid = len(csv_valid_rows) + len(sql_valid_rows)
            total_invalid = len(csv_invalid_rows) + len(sql_invalid_rows)
            
            self.stats["consumed"][entity_type] = consumed
            self.stats["valid"][entity_type] = total_valid
            self.stats["invalid"][entity_type] = total_invalid
            
            logger.info("   Consumed: %s | Valid: %s | Invalid: %s", consumed, total_valid, total_invalid)
            
            # Transform & Load trực tiếp (không ghi file)
            if csv_valid_rows:
                self.transform_and_load(entity_type, csv_valid_rows, source="csv")
            
            if sql_valid_rows:
                self.transform_and_load(entity_type, sql_valid_rows, source="sql")
    
    def transform_and_load(self, entity_type: str, rows: List[Dict], source: str = "csv"):
        """Transform và load trực tiếp vào SQL Server."""
        logger.info("   🔄 Transform & Load [%s]...", source.upper())
        
        # Transform
        transformed_rows = []
        for row in rows:
            try:
                transformed = DataTransformer.transform(entity_type, row)
                transformed_rows.append(transformed)
            except Exception as e:
                logger.error("   Lỗi transform: %s", e)
        
        if not transformed_rows:
            return
        
        # Load vào staging table
        suffix = "_csv" if source == "csv" else "_sql"
        staging_table = f"staging.{entity_type}{suffix}"
        
        try:
            loaded = self.target_db.bulk_insert(
                table_name=staging_table,
                data=transformed_rows,
                batch_size=1000,
            )
            stats_key = f"{entity_type}_{source}"
            self.stats["loaded"][stats_key] = loaded
            logger.info("   ✅ Loaded: %s rows → %s", loaded, staging_table)
            
        except Exception as e:
            logger.error("   ❌ Lỗi load: %s", e)
    
    def infer_entity_type(self, name: str) -> str:
        """Infer entity type từ table name."""
        mapping = {
            "khachhang": "khach_hang",
            "khach_hang": "khach_hang",
            "khach_hang_tbl": "khach_hang",
            "mon": "mon",
            "mon_tbl": "mon",
            "nguyenlieu": "nguyen_lieu",
            "nguyen_lieu": "nguyen_lieu",
            "nguyen_lieu_tbl": "nguyen_lieu",
            "loaimon": "loai_mon",
            "loai_mon": "loai_mon",
            "loai_mon_tbl": "loai_mon",
            "dathang": "dat_hang",
            "dat_hang": "dat_hang",
            "dat_hang_tbl": "dat_hang",
        }
        normalized = name.lower().replace("-", "_").replace(" ", "_")
        return mapping.get(normalized, normalized)
    
    def print_summary(self):
        logger.info("\n" + "=" * 80)
        logger.info("📊 PIPELINE SUMMARY")
        logger.info("=" * 80)
        
        logger.info("\n1️⃣  PRODUCED: %s messages", sum(self.stats["produced"].values()))
        logger.info("2️⃣  CONSUMED: %s messages", sum(self.stats["consumed"].values()))
        logger.info("3️⃣  VALID: %s records", sum(self.stats["valid"].values()))
        logger.info("4️⃣  INVALID: %s records", sum(self.stats["invalid"].values()))
        logger.info("5️⃣  LOADED: %s records", sum(self.stats["loaded"].values()))
        
        logger.info("\n💾 Database: %s", self.db_name)
        logger.info("   Staging tables: staging.*_csv, staging.*_sql")
        
        logger.info("\n✅ PIPELINE HOÀN THÀNH!")
        logger.info("=" * 80)


def main():
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 24 + "PIPELINE DIRECT LOAD" + " " * 35 + "║")
    print("║" + " " * 15 + "Producer → Consumer → Validate → Transform → Load" + " " * 14 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    pipeline = DirectLoadPipeline()
    
    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Pipeline bị dừng bởi user (Ctrl+C)")
    except Exception as e:
        logger.error("❌ Pipeline thất bại: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()
