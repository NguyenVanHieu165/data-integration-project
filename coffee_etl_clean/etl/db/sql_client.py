# etl/db/sql_client.py
import pyodbc
from typing import List, Dict, Optional, Any
from ..logger import logger
from ..utils.retry import retry


class SQLServerClient:
    """Client để kết nối và tương tác với SQL Server."""
    
    def __init__(
        self,
        server: str,
        database: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver: str = "ODBC Driver 17 for SQL Server",
        trusted_connection: bool = False
    ):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.trusted_connection = trusted_connection
        self.connection: Optional[pyodbc.Connection] = None
        self.cursor: Optional[pyodbc.Cursor] = None    
    @retry(times=3, delay_sec=2, label="sql_connect")
    def connect(self):
        """Kết nối tới SQL Server."""
        if self.trusted_connection:
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
            )
        
        self.connection = pyodbc.connect(conn_str)
        self.cursor = self.connection.cursor()
        logger.info(
            "Kết nối SQL Server thành công: %s/%s",
            self.server,
            self.database
        )
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """
        Thực thi SELECT query và trả về kết quả.
        
        Args:
            query: SQL query
            params: Parameters cho query
        
        Returns:
            List of dict (mỗi row là 1 dict)
        """
        if not self.cursor:
            raise RuntimeError("Chưa kết nối SQL Server")
        
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            columns = [column[0] for column in self.cursor.description]
            results = []
            
            for row in self.cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            logger.info(
                "Query thành công, trả về %s rows",
                len(results)
            )
            return results
        
        except pyodbc.Error as e:
            logger.error("Lỗi execute query: %s", e)
            raise
    
    def execute_non_query(self, query: str, params: Optional[tuple] = None) -> int:
        """
        Thực thi INSERT/UPDATE/DELETE query.
        
        Returns:
            Số rows bị ảnh hưởng
        """
        if not self.cursor:
            raise RuntimeError("Chưa kết nối SQL Server")
        
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            self.connection.commit()
            rowcount = self.cursor.rowcount
            
            logger.info("Query thành công, %s rows affected", rowcount)
            return rowcount
        
        except pyodbc.Error as e:
            self.connection.rollback()
            logger.error("Lỗi execute non-query: %s", e)
            raise
    
    def bulk_insert(
        self,
        table_name: str,
        data: List[Dict],
        batch_size: int = 1000
    ) -> int:
        """
        Insert nhiều rows vào table.
        
        Args:
            table_name: Tên table
            data: List of dict (mỗi dict là 1 row)
            batch_size: Số rows insert mỗi batch
        
        Returns:
            Tổng số rows đã insert
        """
        if not data:
            return 0
        
        if not self.cursor:
            raise RuntimeError("Chưa kết nối SQL Server")
        
        # Lấy columns từ dict đầu tiên
        columns = list(data[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        # Escape column names với [] để tránh reserved keywords
        column_names = ", ".join([f"[{col}]" for col in columns])
        
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        total_inserted = 0
        errors = 0
        
        try:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                try:
                    # Chuẩn bị values cho batch
                    values = [
                        tuple(row.get(col) for col in columns)
                        for row in batch
                    ]
                    
                    self.cursor.executemany(query, values)
                    self.connection.commit()
                    
                    total_inserted += len(batch)
                    
                    if total_inserted % (batch_size * 10) == 0:
                        logger.info(
                            "Đã insert %s/%s rows vào %s",
                            total_inserted,
                            len(data),
                            table_name
                        )
                
                except pyodbc.Error as e:
                    self.connection.rollback()
                    errors += len(batch)
                    logger.error(
                        "Lỗi insert batch vào %s: %s",
                        table_name,
                        e
                    )
            
            logger.info(
                "Bulk insert hoàn thành: %s thành công, %s lỗi",
                total_inserted,
                errors
            )
            return total_inserted
        
        except Exception as e:
            logger.error("Lỗi bulk insert: %s", e)
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """Kiểm tra table có tồn tại không."""
        query = """
            SELECT COUNT(*) as cnt
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = ?
        """
        result = self.execute_query(query, (table_name,))
        return result[0]["cnt"] > 0
    
    def truncate_table(self, table_name: str):
        """Xóa toàn bộ dữ liệu trong table."""
        query = f"TRUNCATE TABLE {table_name}"
        self.execute_non_query(query)
        logger.info("Đã truncate table: %s", table_name)
    
    def close(self):
        """Đóng kết nối."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Đã đóng kết nối SQL Server")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
