# etl/transformers/data_transformer.py
"""
Data Transformer - Chuẩn hóa dữ liệu trước khi load
"""
from typing import Dict, Optional
from datetime import datetime


class DataTransformer:
    """Transform dữ liệu theo entity type."""
    
    @staticmethod
    def transform_khach_hang(row: Dict) -> Dict:
        """Transform dữ liệu khách hàng."""
        transformed = {}
        
        # customer_id - Lấy từ 'id' hoặc 'customer_id'
        # KHÔNG thêm 'id' vì đó là IDENTITY column trong SQL
        customer_id = row.get("id") or row.get("customer_id", "")
        transformed["customer_id"] = str(customer_id) if customer_id else None
        
        # Họ tên: Chuẩn hóa khoảng trắng
        ho_ten = str(row.get("ho_ten", "")).strip()
        if ho_ten:
            ho_ten = " ".join(ho_ten.split())
            transformed["ho_ten"] = ho_ten
        else:
            transformed["ho_ten"] = None
        
        # SĐT: Loại bỏ ký tự không phải số
        sdt = str(row.get("sdt", "")).strip()
        transformed["sdt"] = "".join(c for c in sdt if c.isdigit()) if sdt else None
        
        # Thành phố
        thanh_pho = str(row.get("thanh_pho", "")).strip()
        transformed["thanh_pho"] = thanh_pho if thanh_pho else None
        
        # Email: Lowercase
        email = str(row.get("email", "")).strip()
        transformed["email"] = email.lower() if email else None
        
        # Timestamp
        transformed["extract_time"] = datetime.now()
        
        return transformed
    
    @staticmethod
    def transform_nguyen_lieu(row: Dict) -> Dict:
        """Transform dữ liệu nguyên liệu."""
        transformed = {}
        
        transformed["ma_nguyen_lieu"] = str(row.get("ma_nguyen_lieu", ""))
        transformed["ten_nguyen_lieu"] = str(row.get("ten_nguyen_lieu", "")).strip()
        transformed["don_vi"] = str(row.get("don_vi", "")).strip()
        
        # Convert số
        try:
            transformed["so_luong"] = float(row.get("so_luong", 0))
        except (ValueError, TypeError):
            transformed["so_luong"] = 0
        
        try:
            transformed["gia"] = float(row.get("gia", 0))
        except (ValueError, TypeError):
            transformed["gia"] = 0
        
        # Nhà cung cấp
        transformed["nha_cung_cap"] = str(row.get("nha_cung_cap", "")).strip()
        
        # Ngày nhập
        transformed["ngay_nhap"] = row.get("ngay_nhap")
        
        # Timestamp
        extract_time = row.get("extract_time")
        if isinstance(extract_time, str):
            try:
                transformed["extract_time"] = datetime.fromisoformat(extract_time.replace('Z', '+00:00'))
            except:
                transformed["extract_time"] = datetime.now()
        else:
            transformed["extract_time"] = extract_time if extract_time else datetime.now()
        
        return transformed
    
    @staticmethod
    def transform_loai_mon(row: Dict) -> Dict:
        """Transform dữ liệu loại món."""
        # Timestamp
        extract_time = row.get("extract_time")
        if isinstance(extract_time, str):
            try:
                extract_time = datetime.fromisoformat(extract_time.replace('Z', '+00:00'))
            except:
                extract_time = datetime.now()
        else:
            extract_time = extract_time if extract_time else datetime.now()
        
        return {
            "ma_loai": str(row.get("ma_loai", "")),
            "ten_loai": str(row.get("ten_loai", "")).strip(),
            "mo_ta": str(row.get("mo_ta", "")).strip(),
            "extract_time": extract_time
        }
    
    @staticmethod
    def transform_mon(row: Dict) -> Dict:
        """
        Transform dữ liệu món ăn.
        
        Hỗ trợ 2 format:
        1. CSV (tensanpham.csv): ten_san_pham, gia, loai
        2. SQL (mon_tbl): id, ten_mon, loai_id, gia
        """
        transformed = {}
        
        # Phát hiện format: CSV hay SQL?
        if "ten_san_pham" in row:
            # CSV format: ten_san_pham, gia, loai
            transformed["ten_mon"] = str(row.get("ten_san_pham", "")).strip()
            
            # loai là tên loại (string), không phải ID
            # Để None vì staging table không có loai_name column
            transformed["loai_id"] = None
            # Không thêm loai_name vào transformed vì staging table không có cột này
        else:
            # SQL format: ten_mon, loai_id
            transformed["ten_mon"] = str(row.get("ten_mon", "")).strip()
            
            # Loại ID (foreign key)
            try:
                transformed["loai_id"] = int(row.get("loai_id", 0))
            except (ValueError, TypeError):
                transformed["loai_id"] = None
        
        # Convert giá (chung cho cả 2 format)
        try:
            transformed["gia"] = float(row.get("gia", 0))
        except (ValueError, TypeError):
            transformed["gia"] = 0
        
        # Timestamp
        extract_time = row.get("extract_time")
        if isinstance(extract_time, str):
            try:
                transformed["extract_time"] = datetime.fromisoformat(extract_time.replace('Z', '+00:00'))
            except:
                transformed["extract_time"] = datetime.now()
        else:
            transformed["extract_time"] = extract_time if extract_time else datetime.now()
        
        return transformed
    
    @staticmethod
    def transform_dat_hang(row: Dict) -> Dict:
        """Transform dữ liệu đặt hàng."""
        transformed = {}
        
        # KHÔNG thêm 'id' vì đó là IDENTITY column trong SQL
        # Chỉ thêm các foreign keys
        
        khach_hang_id = row.get("khach_hang_id", "")
        transformed["khach_hang_id"] = str(khach_hang_id) if khach_hang_id else None
        
        mon_id = row.get("mon_id", "")
        transformed["mon_id"] = str(mon_id) if mon_id else None
        
        # Convert số lượng
        try:
            so_luong = row.get("so_luong", 0)
            transformed["so_luong"] = int(so_luong) if so_luong else 0
        except (ValueError, TypeError):
            transformed["so_luong"] = 0
        
        # Ngày đặt - giữ nguyên format
        ngay_dat = row.get("ngay_dat")
        transformed["ngay_dat"] = ngay_dat if ngay_dat else None
        
        # Trạng thái
        trang_thai = str(row.get("trang_thai", "")).strip()
        transformed["trang_thai"] = trang_thai if trang_thai else None
        
        # Timestamp
        transformed["extract_time"] = datetime.now()
        
        return transformed
    
    @staticmethod
    def transform(entity_type: str, row: Dict) -> Dict:
        """
        Transform dữ liệu theo entity type.
        
        Args:
            entity_type: Loại entity (khach_hang, nguyen_lieu, ...)
            row: Dữ liệu gốc
        
        Returns:
            Dữ liệu đã transform
        """
        transformers = {
            "khach_hang": DataTransformer.transform_khach_hang,
            "nguyen_lieu": DataTransformer.transform_nguyen_lieu,
            "loai_mon": DataTransformer.transform_loai_mon,
            "mon": DataTransformer.transform_mon,
            "dat_hang": DataTransformer.transform_dat_hang
        }
        
        transformer = transformers.get(entity_type)
        
        if transformer:
            return transformer(row)
        else:
            # Không có transformer -> pass through
            return row
