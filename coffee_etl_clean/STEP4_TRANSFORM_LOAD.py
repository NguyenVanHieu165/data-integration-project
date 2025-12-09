"""
STEP 4: TRANSFORM & LOAD
=========================
Đọc staging/clean/*.csv → Transform → Load vào SQL Server staging tables

Luồng:
- staging/clean/*.csv → Transform → SQL Server staging tables

Output: Data trong SQL Server staging tables
- staging.khach_hang_csv / staging.khach_hang_sql
- staging.loai_mon_csv / staging.loai_mon_sql
- staging.mon_csv / staging.mon_sql
- staging.nguyen_lieu_csv / staging.nguyen_lieu_sql
- staging.dat_hang_csv / staging.dat_hang_sql
"""

import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from etl.db.sql_client import SQLServerClient
from etl.transformers.data_transformer import DataTransformer
from etl.config import settings
from etl.logger import logger


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
            logger.info("   ✅ Đã tạo staging tables (CSV + SQL)")
            return True
            
        except Exception as e:
            logger.error("   ❌ Lỗi tạo tables: %s", e)
            return False


class TransformLoadPipeline:
    """Pipeline Transform & Load - Transform và load vào SQL Server."""
    
    def __init__(self, db_name: str = None, validated_data: Dict = None):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.db_name = db_name or f"DB_{self.run_id}"
        
        self.clean_dir = Path("staging") / "clean"
        self.target_db = None
        
        self.stats = {}
        
        # Nhận validated data từ STEP 3 (nếu có)
        self.validated_data = validated_data or {}
    
    def run(self, valid_data_from_memory: Dict[str, List[Dict]] = None):
        """
        Chạy pipeline Transform & Load.
        
        Args:
            valid_data_from_memory: Dict chứa valid data từ STEP 3 (pipeline mode)
                Format: {
                    "khach_hang_csv": [row1, row2, ...],
                    "khach_hang_sql": [row1, row2, ...],
                    ...
                }
                Nếu None, sẽ đọc từ staging/clean/*.csv (standalone mode)
        """
        logger.info("=" * 80)
        logger.info("STEP 4: TRANSFORM & LOAD PIPELINE")
        logger.info("Run ID: %s", self.run_id)
        logger.info("Database: %s", self.db_name)
        
        if valid_data_from_memory:
            logger.info("Mode: Pipeline (data từ memory)")
        else:
            logger.info("Mode: Standalone (đọc từ staging/clean/)")
        
        logger.info("=" * 80)
        
        try:
            # Setup database
            logger.info("\n🔹 Setup Database")
            logger.info("-" * 80)
            self.setup_database()
            
            logger.info("\n🔹 Transform & Load")
            logger.info("-" * 80)
            
            if valid_data_from_memory:
                # Pipeline mode: Xử lý data từ memory
                self.process_from_memory(valid_data_from_memory)
            else:
                # Standalone mode: Đọc từ files
                self.process_from_files()
            
            self.print_summary()
            
        except Exception as e:
            logger.error("❌ Lỗi Transform & Load pipeline: %s", e, exc_info=True)
            raise
        finally:
            if self.target_db:
                self.target_db.close()
    
    def process_from_memory(self, valid_data: Dict[str, List[Dict]]):
        """Xử lý data trực tiếp từ memory (pipeline mode)."""
        logger.info("📦 Processing %s entities từ memory", len(valid_data))
        
        for entity_source, rows in sorted(valid_data.items()):
            if not rows:
                continue
            
            # Parse entity_source: "khach_hang_csv" → entity="khach_hang", source="csv"
            parts = entity_source.rsplit("_", 1)
            if len(parts) == 2:
                entity_type, source = parts
            else:
                entity_type = entity_source
                source = "unknown"
            
            logger.info("\n📥 Processing: %s (source: %s)", entity_type, source)
            logger.info("   Total rows: %s (tất cả đã pass validation)", len(rows))
            
            # Transform
            self.transform_and_load_rows(entity_type, source, rows)
    
    def process_from_files(self):
        """Xử lý data từ files (standalone mode)."""
        # Tìm tất cả clean files
        clean_files = list(self.clean_dir.glob("*.csv"))
        
        if not clean_files:
            logger.warning("⚠️  Không tìm thấy file nào trong staging/clean/")
            return
        
        logger.info("Found %s clean files", len(clean_files))
        
        for clean_file in sorted(clean_files):
            logger.info("\n📥 Processing: %s", clean_file.name)
            self.process_file(clean_file)
    
    def setup_database(self):
        """Setup database và staging tables."""
        # Kết nối master để tạo database
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
    
    def process_file(self, clean_file: Path):
        """Xử lý một clean file."""
        # Parse file name: entity_source_runid.csv
        file_name = clean_file.stem
        parts = file_name.split("_")
        
        # Xác định entity type và source
        if len(parts) >= 2:
            if parts[-2].isdigit():  # Có run_id
                source = parts[-3]
                entity_type = "_".join(parts[:-3])
            else:
                source = parts[-1]
                entity_type = "_".join(parts[:-1])
        else:
            logger.warning("   ⚠️  Không parse được file name: %s", file_name)
            return
        
        logger.info("   Entity: %s | Source: %s", entity_type, source)
        
        # Đọc clean file
        # CHÚ Ý: Chỉ đọc từ staging/clean/ - các rows đã pass validation
        # Các rows có lỗi (bao gồm cột rỗng) đã bị loại bỏ ở STEP 3
        try:
            with open(clean_file, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except Exception as e:
            logger.error("   ✗ Lỗi đọc file: %s", e)
            return
        
        logger.info("   Total rows: %s (tất cả đã pass validation)", len(rows))
        
        if not rows:
            logger.info("   ⚠️  File rỗng")
            return
        
        # Transform
        # CHÚ Ý: Chỉ transform các rows VALID từ clean zone
        logger.info("   🔄 Transforming...")
        transformed_rows = []
        for row in rows:
            try:
                transformed = DataTransformer.transform(entity_type, row)
                transformed_rows.append(transformed)
            except Exception as e:
                logger.error("   ✗ Lỗi transform row: %s", e)
        
        logger.info("   ✓ Transformed: %s rows", len(transformed_rows))
        
        # Load vào staging table
        # CHÚ Ý: Chỉ load các rows đã pass validation và transform thành công
        if transformed_rows:
            staging_table = f"staging.{entity_type}_{source}"
            
            try:
                loaded = self.target_db.bulk_insert(
                    table_name=staging_table,
                    data=transformed_rows,
                    batch_size=1000
                )
                logger.info("   ✅ Loaded: %s rows → %s", loaded, staging_table)
                
                self.stats[file_name] = {
                    "entity": entity_type,
                    "source": source,
                    "total": len(rows),
                    "loaded": loaded
                }
                
            except Exception as e:
                logger.error("   ❌ Lỗi load: %s", e)
    
    def transform_and_load_rows(self, entity_type: str, source: str, rows: List[Dict]):
        """Transform và load rows vào SQL Server."""
        # Transform
        # CHÚ Ý: Chỉ transform các rows VALID từ memory
        logger.info("   🔄 Transforming...")
        transformed_rows = []
        for row in rows:
            try:
                transformed = DataTransformer.transform(entity_type, row)
                transformed_rows.append(transformed)
            except Exception as e:
                logger.error("   ✗ Lỗi transform row: %s", e)
        
        logger.info("   ✓ Transformed: %s rows", len(transformed_rows))
        
        # Load vào staging table
        # CHÚ Ý: Chỉ load các rows đã pass validation và transform thành công
        if transformed_rows:
            staging_table = f"staging.{entity_type}_{source}"
            
            try:
                loaded = self.target_db.bulk_insert(
                    table_name=staging_table,
                    data=transformed_rows,
                    batch_size=1000
                )
                logger.info("   ✅ Loaded: %s rows → %s", loaded, staging_table)
                
                self.stats[f"{entity_type}_{source}"] = {
                    "entity": entity_type,
                    "source": source,
                    "total": len(rows),
                    "loaded": loaded
                }
                
            except Exception as e:
                logger.error("   ❌ Lỗi load: %s", e)
    
    def print_summary(self):
        logger.info("\n" + "=" * 80)
        logger.info("📊 TRANSFORM & LOAD SUMMARY")
        logger.info("=" * 80)
        
        logger.info("\n💾 Database: %s", self.db_name)
        
        logger.info("\n📊 Loaded Data:")
        total_loaded = 0
        
        for file_name, stats in sorted(self.stats.items()):
            logger.info("   • staging.%s_%s: %s rows", 
                       stats["entity"], 
                       stats["source"], 
                       stats["loaded"])
            total_loaded += stats["loaded"]
        
        logger.info("\n✅ TỔNG: %s rows đã load vào SQL Server", total_loaded)
        logger.info("=" * 80)


def main():
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 24 + "STEP 4: TRANSFORM & LOAD" + " " * 31 + "║")
    print("║" + " " * 18 + "staging/clean/ → SQL Server Staging" + " " * 26 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    # Có thể truyền db_name từ command line hoặc dùng mặc định
    import sys
    db_name = sys.argv[1] if len(sys.argv) > 1 else None
    
    pipeline = TransformLoadPipeline(db_name=db_name)
    
    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Pipeline bị dừng bởi user (Ctrl+C)")
    except Exception as e:
        logger.error("❌ Pipeline thất bại: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()
