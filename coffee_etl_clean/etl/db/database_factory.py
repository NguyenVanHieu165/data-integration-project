# etl/db/database_factory.py
from .sql_client import SQLServerClient
from ..config import settings
from ..logger import logger


class DatabaseFactory:
    """Factory để tạo kết nối tới các database."""
    
    @staticmethod
    def create_source_db() -> SQLServerClient:
        """
        Tạo kết nối tới Source Database (ComVanPhong).
        Đây là database nội bộ chứa dữ liệu thô.
        """
        logger.info("Tạo kết nối tới Source DB: %s", settings.SOURCE_DB_NAME)
        
        return SQLServerClient(
            server=f"{settings.SOURCE_DB_HOST},{settings.SOURCE_DB_PORT}",
            database=settings.SOURCE_DB_NAME,
            driver=settings.SOURCE_DB_DRIVER,
            trusted_connection=settings.SOURCE_DB_TRUSTED_CONNECTION
        )
    
    @staticmethod
    def create_target_db() -> SQLServerClient:
        """
        Tạo kết nối tới Target Database (newdata).
        Đây là database đích chứa staging và DWH.
        """
        logger.info("Tạo kết nối tới Target DB: %s", settings.TARGET_DB_NAME)
        
        return SQLServerClient(
            server=f"{settings.TARGET_DB_HOST},{settings.TARGET_DB_PORT}",
            database=settings.TARGET_DB_NAME,
            driver=settings.TARGET_DB_DRIVER,
            trusted_connection=settings.TARGET_DB_TRUSTED_CONNECTION
        )


class SourceDBReader:
    """Reader để đọc dữ liệu từ Source Database (ComVanPhong)."""
    
    def __init__(self, sql_client: SQLServerClient):
        self.sql_client = sql_client
    
    def get_all_tables(self, schema: str = "dbo"):
        """
        Lấy danh sách tất cả tables trong database.
        
        Args:
            schema: Schema name (mặc định: dbo)
        
        Returns:
            List of table names
        """
        query = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME
        """
        
        logger.info("Lấy danh sách tables từ schema: %s", schema)
        results = self.sql_client.execute_query(query, (schema,))
        
        tables = [row["TABLE_NAME"] for row in results]
        logger.info("Tìm thấy %s tables: %s", len(tables), ", ".join(tables))
        
        return tables
    
    def get_table_info(self, table_name: str, schema: str = "dbo"):
        """
        Lấy thông tin về columns của table.
        
        Returns:
            Dict với keys: columns, row_count
        """
        # Lấy columns
        query_columns = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            AND TABLE_SCHEMA = ?
            ORDER BY ORDINAL_POSITION
        """
        
        columns = self.sql_client.execute_query(query_columns, (table_name, schema))
        
        # Đếm số rows
        query_count = f"SELECT COUNT(*) as row_count FROM {schema}.{table_name}"
        count_result = self.sql_client.execute_query(query_count)
        row_count = count_result[0]["row_count"] if count_result else 0
        
        return {
            "table_name": table_name,
            "schema": schema,
            "columns": columns,
            "row_count": row_count
        }
    
    def read_table(self, table_name: str, schema: str = "dbo", limit: int = None):
        """
        Đọc dữ liệu từ bất kỳ table nào.
        
        Args:
            table_name: Tên table
            schema: Schema name
            limit: Giới hạn số rows
        
        Returns:
            List of dict (rows)
        """
        query = f"SELECT * FROM {schema}.{table_name}"
        if limit:
            query = f"SELECT TOP {limit} * FROM {schema}.{table_name}"
        
        logger.info("Đọc dữ liệu từ %s.%s", schema, table_name)
        return self.sql_client.execute_query(query)
    
    def read_all_tables(self, schema: str = "dbo", limit: int = None):
        """
        Đọc dữ liệu từ TẤT CẢ tables trong database.
        
        Args:
            schema: Schema name
            limit: Giới hạn số rows mỗi table
        
        Returns:
            Dict: {table_name: [rows]}
        """
        tables = self.get_all_tables(schema)
        
        results = {}
        for table_name in tables:
            try:
                data = self.read_table(table_name, schema, limit)
                results[table_name] = data
                logger.info("✓ Đọc %s rows từ %s", len(data), table_name)
            except Exception as e:
                logger.error("✗ Lỗi đọc table %s: %s", table_name, e)
                results[table_name] = []
        
        return results
    
    # Giữ lại các method cũ để tương thích ngược
    def read_nguyen_lieu_tho(self, limit: int = None):
        """Đọc dữ liệu từ table nguyên_liệu_thô."""
        return self.read_table("nguyen_lieu_tho", "dbo", limit)
    
    def read_loai_mon_tho(self, limit: int = None):
        """Đọc dữ liệu từ table loai_mon_thô."""
        return self.read_table("loai_mon_tho", "dbo", limit)
    
    def read_dat_hang_tho(self, limit: int = None):
        """Đọc dữ liệu từ table dat_hang_thô."""
        return self.read_table("dat_hang_tho", "dbo", limit)
    
    def read_custom_query(self, query: str):
        """Thực thi custom query trên Source DB."""
        logger.info("Thực thi custom query trên Source DB")
        return self.sql_client.execute_query(query)
