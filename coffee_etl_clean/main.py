

import time
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from etl.broker.rabbitmq_client import RabbitMQClient
from etl.db.database_factory import DatabaseFactory, SourceDBReader
from etl.readers.csv_staging_reader import csv_staging_reader
from etl.quality.rule_registry import rule_registry
from etl.transformers.data_transformer import DataTransformer
from etl.config import settings
from etl.logger import logger


# =============================================================================
#  LOGGER THEO TỪNG ENTITY
# =============================================================================

class EntityLogger:
    """Logger riêng cho từng entity/bảng (log chi tiết lỗi validation)."""

    def __init__(self, run_id: str):
        self.run_id = run_id
        # Tạo folder log theo run_id: logs/run_YYYYMMDD_HHMMSS/
        self.log_dir = Path("logs") / f"run_{run_id}"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.entity_log_files: Dict[str, Path] = {}
        self.entity_stats: Dict[str, Dict[str, int]] = {}

    def _get_log_file(self, entity_type: str) -> Path:
        """Lấy / tạo file log cho một entity."""
        if entity_type not in self.entity_log_files:
            log_file = self.log_dir / f"{entity_type}_validation.log"
            self.entity_log_files[entity_type] = log_file
            with open(log_file, "w", encoding="utf-8") as f:
                f.write(f"# Validation Log for {entity_type}\n")
                f.write(f"# Run ID: {self.run_id}\n")
                f.write(f"# Created: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
                f.write("#" + "=" * 78 + "\n\n")
        return self.entity_log_files[entity_type]

    def log_invalid_row(
        self,
        entity_type: str,
        row_number: int,
        errors: List[str],
        data: Dict,
    ) -> None:
        """Ghi log 1 record invalid vào file của entity."""
        log_file = self._get_log_file(entity_type)

        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "row_number": row_number,
            "errors": errors,
            "data": data,
        }

        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, indent=2) + "\n\n")

        self.entity_stats.setdefault(entity_type, {"invalid_count": 0})
        self.entity_stats[entity_type]["invalid_count"] += 1

    def get_summary(self):
        return {"log_files": self.entity_log_files, "stats": self.entity_stats}


# =============================================================================
#  DATABASE MANAGER (TỪ BẢN 2 – ĐÃ CÓ *_csv VÀ *_sql)
# =============================================================================

class DatabaseManager:
    """Quản lý việc tạo database + schema + staging tables."""

    @staticmethod
    def create_database(db_name: str, sql_client):
        """Tạo database mới nếu chưa tồn tại."""
        try:
            check_query = f"""
            SELECT database_id 
            FROM sys.databases 
            WHERE name = '{db_name}'
            """
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
        """Tạo các staging tables cho cả CSV và SQL."""
        # Tạo từng table riêng lẻ (không dùng GO statements)
        tables = [
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
            logger.info("   ✅ Đã tạo staging tables (CSV + SQL)")
            return True

        except Exception as e:
            logger.error("   ❌ Lỗi tạo tables: %s", e)
            return False


# =============================================================================
#  FAILED DATA LOGGER (TỪ BẢN 2)
# =============================================================================

class FailedDataLogger:
    """Lưu tất cả record fail vào 1 file CSV tổng."""

    def __init__(self, run_id: str):
        self.run_id = run_id
        # Tạo folder log theo run_id: logs/run_YYYYMMDD_HHMMSS/
        self.log_dir = Path("logs") / f"run_{run_id}"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.failed_file = self.log_dir / "failed_data.csv"
        self.failed_data = []

    def add_failed_record(self, entity_type: str, data: dict, errors: List[str]):
        self.failed_data.append(
            {
                "entity": entity_type,
                "data": data,
                "errors": errors,
                "time": datetime.now().strftime("%H:%M:%S"),
            }
        )

    def save_to_file(self):
        if not self.failed_data:
            logger.info("   Không có dữ liệu fail")
            return

        try:
            import csv

            with open(
                self.failed_file, "w", encoding="utf-8-sig", newline=""
            ) as f:
                writer = csv.writer(f)
                writer.writerow(["Time", "Entity", "Errors", "Data"])

                for record in self.failed_data:
                    time_str = record["time"]
                    entity = record["entity"]
                    errors_str = "; ".join(record["errors"])
                    important_data = self._get_important_fields(
                        entity, record["data"]
                    )
                    writer.writerow([time_str, entity, errors_str, important_data])

            logger.info(
                "   ✅ Đã lưu %s failed records vào: %s",
                len(self.failed_data),
                self.failed_file.relative_to(Path.cwd()),
            )

        except Exception as e:
            logger.error("   ❌ Lỗi lưu failed data: %s", e)

    def _get_important_fields(self, entity: str, data: dict) -> str:
        skip_fields = {
            "source_system",
            "file",
            "line",
            "extract_time",
            "loaded_at",
            "updated_at",
        }

        if entity == "khach_hang":
            fields = ["customer_id", "ho_ten", "sdt", "email", "thanh_pho"]
        elif entity == "nguyen_lieu":
            fields = [
                "ma_nguyen_lieu",
                "ten_nguyen_lieu",
                "so_luong",
                "don_vi",
                "gia",
                "nha_cung_cap",
            ]
        elif entity == "mon":
            fields = ["ten_mon", "loai_id", "gia"]
        elif entity == "loai_mon":
            fields = ["ma_loai", "ten_loai", "mo_ta"]
        elif entity == "dat_hang":
            fields = ["id", "khach_hang_id", "mon_id", "so_luong", "ngay_dat", "trang_thai"]
        else:
            fields = [k for k in data.keys() if k not in skip_fields]

        important = {k: data.get(k) for k in fields if k in data}
        return " | ".join(f"{k}={v}" for k, v in important.items())


# =============================================================================
#  MAIN PIPELINE (KẾT HỢP)
# =============================================================================

class MainETLPipeline:
    """Pipeline ETL chính thức – đã merge hai phiên bản."""

    def __init__(self):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.db_name = f"DB_{self.run_id}"
        self.target_db = None

        self.failed_logger = FailedDataLogger(self.run_id)
        self.entity_logger = EntityLogger(self.run_id)

        self.stats = {
            "produced": {},
            "consumed": {},
            "valid": {},
            "invalid": {},
            "loaded": {},
        }

    # -------------------------------------------------------------------------
    # ENTRY
    # -------------------------------------------------------------------------

    def run(self):
        logger.info("=" * 80)
        logger.info(
            "MAIN ETL PIPELINE - Producer → RabbitMQ → Consumer → Validate + Transform → Load"
        )
        logger.info("Run ID: %s", self.run_id)
        logger.info("Database: %s", self.db_name)
        logger.info("=" * 80)

        try:
            logger.info("\n🔹 PHASE 0: TẠO DATABASE MỚI")
            logger.info("-" * 80)
            self.setup_database()

            logger.info("\n🔹 PHASE 1: PRODUCER → RabbitMQ")
            logger.info("-" * 80)
            self.producer_phase()

            logger.info("\n⏳ Đợi 2 giây để message được gửi xong...")
            time.sleep(2)

            logger.info(
                "\n🔹 PHASE 2: CONSUMER → Validate + Transform → Load Staging"
            )
            logger.info("-" * 80)
            self.consumer_phase()

            logger.info("\n🔹 PHASE 3: LƯU FAILED DATA")
            logger.info("-" * 80)
            self.failed_logger.save_to_file()

            self.print_summary()
            
            # In đường dẫn log folder
            log_folder = Path("logs") / f"run_{self.run_id}"
            logger.info("\n📁 Log folder: %s", log_folder)

        except Exception as e:
            logger.error("❌ Lỗi pipeline: %s", e, exc_info=True)
            raise
        finally:
            if self.target_db:
                self.target_db.close()

    # -------------------------------------------------------------------------
    # PHASE 0 – DB
    # -------------------------------------------------------------------------

    def setup_database(self):
        from etl.db.sql_client import SQLServerClient

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

        new_db = SQLServerClient(
            server=f"{settings.TARGET_DB_HOST},{settings.TARGET_DB_PORT}",
            database=self.db_name,
            driver=settings.TARGET_DB_DRIVER,
            trusted_connection=settings.TARGET_DB_TRUSTED_CONNECTION,
        )

        try:
            new_db.connect()
            DatabaseManager.create_staging_schema(new_db)
            DatabaseManager.create_staging_tables(new_db)
            logger.info("✅ Setup database hoàn thành")
        finally:
            new_db.close()

    # -------------------------------------------------------------------------
    # PHASE 1 – PRODUCER
    # -------------------------------------------------------------------------

    def producer_phase(self):
        with RabbitMQClient(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            username=settings.RABBITMQ_USER,
            password=settings.RABBITMQ_PASSWORD,
        ) as rabbitmq:

            logger.info("📄 Producer 1: Đọc CSV files...")
            csv_stats = self.produce_from_csv(rabbitmq)
            self.stats["produced"].update(csv_stats)

            logger.info("\n💾 Producer 2: Đọc SQL Server...")
            sql_stats = self.produce_from_sql(rabbitmq)
            self.stats["produced"].update(sql_stats)

            logger.info("\n✅ Producer phase hoàn thành!")
            logger.info(
                "   Tổng: %s messages đã gửi vào RabbitMQ",
                sum(self.stats["produced"].values()),
            )

    def produce_from_csv(self, rabbitmq: RabbitMQClient) -> Dict[str, int]:
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
                logger.info(
                    "   ✓ %s: %s messages → %s", file_name, count, queue_name
                )

            except Exception as e:
                logger.error("   ✗ Lỗi %s: %s", file_name, e)

        return stats

    def produce_from_sql(self, rabbitmq: RabbitMQClient) -> Dict[str, int]:
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

                    from etl.utils.json_encoder import (
                        convert_sql_row_to_json_compatible,
                    )

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
                    logger.info(
                        "   ✓ %s: %s messages → %s", table, len(data), queue_name
                    )

                except Exception as e:
                    logger.error("   ✗ Lỗi %s: %s", table, e)

        finally:
            source_db.close()

        return stats

    # -------------------------------------------------------------------------
    # PHASE 2 – CONSUMER
    # -------------------------------------------------------------------------

    def consumer_phase(self):
        from etl.db.sql_client import SQLServerClient

        logger.info("💾 Kết nối Target DB: %s", self.db_name)
        self.target_db = SQLServerClient(
            server=f"{settings.TARGET_DB_HOST},{settings.TARGET_DB_PORT}",
            database=self.db_name,
            driver=settings.TARGET_DB_DRIVER,
            trusted_connection=settings.TARGET_DB_TRUSTED_CONNECTION,
        )
        self.target_db.connect()

        queues = self.get_queues_to_consume()
        if not queues:
            logger.warning("Không có queue nào để consume")
            return

        for queue_name, entity_type in queues:
            logger.info("\n📥 Processing: %s", queue_name)
            self.consume_and_process(queue_name, entity_type)

        logger.info("\n✅ Consumer phase hoàn thành!")

    def get_queues_to_consume(self) -> List[tuple]:
        return [
            ("queue_khach_hang", "khach_hang"),
            ("queue_loai_mon", "loai_mon"),
            ("queue_mon", "mon"),
            ("queue_nguyen_lieu", "nguyen_lieu"),
            ("queue_dat_hang", "dat_hang"),
        ]

    def consume_and_process(self, queue_name: str, entity_type: str):
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
            invalid_rows = []

            seen_ids = set()
            seen_emails = set()

            def callback(ch, method, properties, body):
                nonlocal consumed

                try:
                    message = json.loads(body.decode("utf-8"))
                    data = message.get("data", {})
                    source = message.get("source", "unknown")

                    is_valid, fixed_row, errors = rule_registry.validate_row(
                        entity_type=entity_type,
                        row=data,
                        context={
                            "existing_ids": seen_ids,
                            "existing_emails": seen_emails,
                            "source": source,  # Thêm source để phân biệt CSV vs SQL
                        },
                    )

                    if is_valid:
                        if source == "csv":
                            csv_valid_rows.append(fixed_row)
                        elif source == "sql":
                            sql_valid_rows.append(fixed_row)
                        else:
                            csv_valid_rows.append(fixed_row)

                        for id_field in ["id", "customer_id"]:
                            if id_field in fixed_row and fixed_row[id_field]:
                                try:
                                    seen_ids.add(int(fixed_row[id_field]))
                                except (ValueError, TypeError):
                                    pass

                        if "email" in fixed_row and fixed_row["email"]:
                            seen_emails.add(fixed_row["email"].lower())

                    else:
                        invalid_rows.append((data, errors))
                        # Ghi ra 2 nơi: file CSV tổng + file JSON theo entity
                        self.failed_logger.add_failed_record(
                            entity_type, data, errors
                        )
                        self.entity_logger.log_invalid_row(
                            entity_type, consumed + 1, errors, data
                        )
                        # KHÔNG log từng lỗi lên console nữa (đỡ ồn)

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    consumed += 1

                except Exception as e:
                    logger.error("   Lỗi xử lý message: %s", e)
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag, requeue=False
                    )

            rabbitmq.channel.basic_qos(prefetch_count=1)
            rabbitmq.channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )

            while consumed < message_count:
                rabbitmq.connection.process_data_events(time_limit=1)

            total_valid = len(csv_valid_rows) + len(sql_valid_rows)
            self.stats["consumed"][entity_type] = consumed
            self.stats["valid"][entity_type] = total_valid
            self.stats["invalid"][entity_type] = len(invalid_rows)

            logger.info(
                "   Consumed: %s | Valid: %s (CSV: %s, SQL: %s) | Invalid: %s",
                consumed,
                total_valid,
                len(csv_valid_rows),
                len(sql_valid_rows),
                len(invalid_rows),
            )

            if csv_valid_rows:
                self.transform_and_load(entity_type, csv_valid_rows, source="csv")

            if sql_valid_rows:
                self.transform_and_load(entity_type, sql_valid_rows, source="sql")

    def transform_and_load(
        self, entity_type: str, rows: List[Dict], source: str = "csv"
    ):
        logger.info("   🔄 Transform & Load [%s]...", source.upper())

        transformed_rows = []
        for row in rows:
            try:
                transformed_rows.append(
                    DataTransformer.transform(entity_type, row)
                )
            except Exception as e:
                logger.error("   Lỗi transform: %s", e)

        if not transformed_rows:
            return

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

    # -------------------------------------------------------------------------
    # HELPERS & SUMMARY
    # -------------------------------------------------------------------------

    def infer_entity_type(self, name: str) -> str:
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
        logger.info("📊 TỔNG KẾT PIPELINE")
        logger.info("=" * 80)

        logger.info("\n1️⃣  PRODUCER → RabbitMQ:")
        for source, count in self.stats["produced"].items():
            logger.info("   • %s: %s messages", source, count)
        logger.info(
            "   TỔNG PRODUCED: %s messages", sum(self.stats["produced"].values())
        )

        logger.info("\n2️⃣  CONSUMER (RabbitMQ → Consumer):")
        for entity, count in self.stats["consumed"].items():
            logger.info("   • %s: %s messages", entity, count)
        logger.info(
            "   TỔNG CONSUMED: %s messages",
            sum(self.stats["consumed"].values()),
        )

        logger.info("\n3️⃣  VALIDATE (Data Quality Rules):")
        total_valid = sum(self.stats["valid"].values())
        total_invalid = sum(self.stats["invalid"].values())
        total = total_valid + total_invalid

        for entity in self.stats["valid"].keys():
            valid = self.stats["valid"].get(entity, 0)
            invalid = self.stats["invalid"].get(entity, 0)
            logger.info("   • %s: Valid=%s | Invalid=%s", entity, valid, invalid)

        if total > 0:
            logger.info(
                "   TỔNG: Valid=%s (%.1f%%) | Invalid=%s (%.1f%%)",
                total_valid,
                total_valid / total * 100,
                total_invalid,
                total_invalid / total * 100,
            )

        logger.info("\n4️⃣  TRANSFORM: Chuẩn hóa %s valid records", total_valid)

        logger.info("\n5️⃣  LOAD (Staging Tables):")
        csv_total = 0
        sql_total = 0
        for key, count in self.stats["loaded"].items():
            if "_csv" in key:
                entity = key.replace("_csv", "")
                logger.info("   • staging.%s_csv: %s rows", entity, count)
                csv_total += count
            elif "_sql" in key:
                entity = key.replace("_sql", "")
                logger.info("   • staging.%s_sql: %s rows", entity, count)
                sql_total += count
            else:
                logger.info("   • staging.%s: %s rows", key, count)

        logger.info(
            "   TỔNG LOADED: %s rows (CSV: %s, SQL: %s)",
            sum(self.stats["loaded"].values()),
            csv_total,
            sql_total,
        )

        # Thông tin file log validation theo entity
        summary = self.entity_logger.get_summary()
        if summary["log_files"]:
            logger.info("\n6️⃣  VALIDATION LOGS THEO TỪNG BẢNG:")
            for entity, log_file in sorted(summary["log_files"].items()):
                invalid_count = summary["stats"].get(entity, {}).get(
                    "invalid_count", 0
                )
                logger.info(
                    "   • %s: %s (%s errors)",
                    entity,
                    log_file.name,
                    invalid_count,
                )

        logger.info("\n" + "=" * 80)
        logger.info("✅ PIPELINE HOÀN THÀNH!")
        logger.info(
            "   Producer → RabbitMQ → Consumer → Validate + Transform → Load Staging"
        )
        logger.info("=" * 80)
        logger.info("")
        logger.info("📁 Logs:")
        logger.info("   • Pipeline: logs/pipeline.log")
        logger.info("   • Data: logs/data.log")
        logger.info("   • Error: logs/error.log")
        logger.info("   • Failed data: logs/failed_data_%s.csv", self.run_id)
        logger.info("   • Validation per-entity: *_validation_%s.log", self.run_id)
        logger.info("")
        logger.info("💾 Database: %s", self.db_name)
        logger.info("   • Staging CSV: staging.*_csv")
        logger.info("   • Staging SQL: staging.*_sql")
        logger.info("")


# =============================================================================
#  MAIN
# =============================================================================

def main():
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 25 + "MAIN ETL PIPELINE" + " " * 36 + "║")
    print(
        "║"
        + " "
        * 10
        + "Producer → RabbitMQ → Consumer → Validate + Transform → Load"
        + " "
        * 7
        + "║"
    )
    print("╚" + "=" * 78 + "╝")
    print()

    pipeline = MainETLPipeline()

    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("")
        logger.info("⚠️  Pipeline bị dừng bởi user (Ctrl+C)")
    except Exception as e:
        logger.error("❌ Pipeline thất bại: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()
