# etl/db/staging_writer.py
from typing import Dict, List
from .sql_client import SQLServerClient
from ..logger import logger


class StagingWriter:
    """Writer để ghi dữ liệu vào staging tables."""
    
    def __init__(self, sql_client: SQLServerClient):
        self.sql_client = sql_client
    
    def write_nguyen_lieu(self, data: Dict) -> bool:
        """Ghi dữ liệu nguyên liệu vào staging."""
        try:
            query = """
                INSERT INTO staging.nguyen_lieu_tbl 
                (ma_nguyen_lieu, ten_nguyen_lieu, don_vi, so_luong, gia, ngay_nhap)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            params = (
                data.get("ma_nguyen_lieu"),
                data.get("ten_nguyen_lieu"),
                data.get("don_vi"),
                data.get("so_luong"),
                data.get("gia"),
                data.get("ngay_nhap")
            )
            
            self.sql_client.execute_non_query(query, params)
            return True
        
        except Exception as e:
            logger.error("Lỗi ghi staging nguyen_lieu: %s", e)
            return False
    
    def write_loai_mon(self, data: Dict) -> bool:
        """Ghi dữ liệu loại món vào staging."""
        try:
            query = """
                INSERT INTO staging.loai_mon_tbl 
                (ma_loai, ten_loai, mo_ta)
                VALUES (?, ?, ?)
            """
            params = (
                data.get("ma_loai"),
                data.get("ten_loai"),
                data.get("mo_ta")
            )
            
            self.sql_client.execute_non_query(query, params)
            return True
        
        except Exception as e:
            logger.error("Lỗi ghi staging loai_mon: %s", e)
            return False
    
    def write_khach_hang(self, data: Dict) -> bool:
        """Ghi dữ liệu khách hàng vào staging."""
        try:
            query = """
                INSERT INTO staging.khach_hang_tbl 
                (customer_id, ho_ten, sdt, thanh_pho, email, source_system, file, line, extract_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                data.get("customer_id"),
                data.get("ho_ten"),
                data.get("sdt"),
                data.get("thanh_pho"),
                data.get("email"),
                data.get("source_system"),
                data.get("file"),
                data.get("line"),
                data.get("extract_time")
            )
            
            self.sql_client.execute_non_query(query, params)
            return True
        
        except Exception as e:
            logger.error("Lỗi ghi staging khach_hang: %s", e)
            return False
    
    def write_dat_hang(self, data: Dict) -> bool:
        """Ghi dữ liệu đặt hàng vào staging."""
        try:
            query = """
                INSERT INTO staging.dat_hang_tbl 
                (ma_don_hang, customer_id, ngay_dat, tong_tien, trang_thai)
                VALUES (?, ?, ?, ?, ?)
            """
            params = (
                data.get("ma_don_hang"),
                data.get("customer_id"),
                data.get("ngay_dat"),
                data.get("tong_tien"),
                data.get("trang_thai")
            )
            
            self.sql_client.execute_non_query(query, params)
            return True
        
        except Exception as e:
            logger.error("Lỗi ghi staging dat_hang: %s", e)
            return False
    
    def bulk_write(self, table_name: str, data: List[Dict]) -> int:
        """
        Ghi nhiều records vào staging table.
        
        Args:
            table_name: Tên table (không có prefix staging.)
            data: List of dict
        
        Returns:
            Số records đã ghi thành công
        """
        full_table_name = f"staging.{table_name}"
        return self.sql_client.bulk_insert(full_table_name, data)


class StagingReader:
    """Reader để đọc dữ liệu từ staging tables."""
    
    def __init__(self, sql_client: SQLServerClient):
        self.sql_client = sql_client
    
    def read_nguyen_lieu(self, limit: int = None) -> List[Dict]:
        """Đọc dữ liệu nguyên liệu từ staging."""
        query = "SELECT * FROM staging.nguyen_lieu_tbl"
        if limit:
            query += f" TOP {limit}"
        
        return self.sql_client.execute_query(query)
    
    def read_loai_mon(self, limit: int = None) -> List[Dict]:
        """Đọc dữ liệu loại món từ staging."""
        query = "SELECT * FROM staging.loai_mon_tbl"
        if limit:
            query += f" TOP {limit}"
        
        return self.sql_client.execute_query(query)
    
    def read_khach_hang(self, limit: int = None) -> List[Dict]:
        """Đọc dữ liệu khách hàng từ staging."""
        query = "SELECT * FROM staging.khach_hang_tbl"
        if limit:
            query += f" TOP {limit}"
        
        return self.sql_client.execute_query(query)
    
    def read_dat_hang(self, limit: int = None) -> List[Dict]:
        """Đọc dữ liệu đặt hàng từ staging."""
        query = "SELECT * FROM staging.dat_hang_tbl"
        if limit:
            query += f" TOP {limit}"
        
        return self.sql_client.execute_query(query)
