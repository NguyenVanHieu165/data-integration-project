"""
Data Quality Rules cho bảng dat_hang_tbl
========================================

Cột: id, khach_hang_id, mon_id, so_luong, ngay_dat, trang_thai
"""
from typing import List, Optional, Set
from datetime import datetime
import re
from ..regex_patterns import (
    ID_PATTERN,
    POSITIVE_NUMBER,
    DATE_ISO,
    DATETIME_ISO,
    TRANG_THAI_DON_HANG,
    TRANG_THAI_CHARSET
)


def _is_blank(value: Optional[str]) -> bool:
    """Kiểm tra giá trị rỗng."""
    return value is None or str(value).strip() == ""


# ===========================================================
# ID VALIDATION - 2 rules
# ===========================================================
def validate_dat_hang_id(raw_id, existing_ids: Optional[Set] = None) -> List[str]:
    """
    Validate ID đặt hàng.
    
    Rules:
    1. Số nguyên dương
    2. Không trùng
    """
    errors = []
    s = str(raw_id) if raw_id is not None else ""
    
    if _is_blank(s):
        errors.append("id: Không rỗng")
        return errors
    
    # Rule 1: Regex
    if not ID_PATTERN.match(s):
        errors.append("id: Phải là số nguyên dương (regex: ^[1-9]\\d*$)")
        return errors
    
    value = int(s)
    
    return errors


# ===========================================================
# KHACH HANG ID VALIDATION - 3 rules
# ===========================================================
def validate_khach_hang_id(value, valid_khach_hang_ids: Optional[Set] = None) -> List[str]:
    """
    Validate khach_hang_id (foreign key).
    
    Rules:
    1. Không rỗng
    2. Số nguyên dương
    3. Foreign key: tồn tại trong khach_hang_tbl.id
    """
    errors = []
    
    # Rule 1: Không rỗng
    if value is None or value == "":
        errors.append("khach_hang_id: Không rỗng")
        return errors
    
    s = str(value).strip()
    
    # Rule 2: Regex
    if not ID_PATTERN.match(s):
        errors.append("khach_hang_id: Phải là số nguyên dương (regex: ^[1-9]\\d*$)")
        return errors
    
    try:
        khach_hang_id = int(s)
        
        # Rule 3: Foreign key
        if valid_khach_hang_ids and khach_hang_id not in valid_khach_hang_ids:
            errors.append("khach_hang_id: Không tồn tại trong khach_hang_tbl")
    
    except (ValueError, TypeError):
        errors.append("khach_hang_id: Không thể convert sang số")
    
    return errors


# ===========================================================
# MON ID VALIDATION - 3 rules
# ===========================================================
def validate_mon_id_fk(value, valid_mon_ids: Optional[Set] = None) -> List[str]:
    """
    Validate mon_id (foreign key).
    
    Rules:
    1. Không rỗng
    2. Số nguyên dương
    3. Foreign key: tồn tại trong mon_tbl.id
    """
    errors = []
    
    # Rule 1: Không rỗng
    if value is None or value == "":
        errors.append("mon_id: Không rỗng")
        return errors
    
    s = str(value).strip()
    
    # Rule 2: Regex
    if not ID_PATTERN.match(s):
        errors.append("mon_id: Phải là số nguyên dương (regex: ^[1-9]\\d*$)")
        return errors
    
    try:
        mon_id = int(s)
        
        # Rule 3: Foreign key
        if valid_mon_ids and mon_id not in valid_mon_ids:
            errors.append("mon_id: Không tồn tại trong mon_tbl")
    
    except (ValueError, TypeError):
        errors.append("mon_id: Không thể convert sang số")
    
    return errors


# ===========================================================
# SO LUONG VALIDATION - 4 rules
# ===========================================================
def validate_so_luong_dat_hang(value) -> List[str]:
    """
    Validate số lượng đặt hàng.
    
    Rules:
    1. Không rỗng
    2. Là số nguyên dương (không thập phân)
    3. ≥ 1
    4. ≤ 1000 (giới hạn business)
    """
    errors = []
    
    # Rule 1: Không rỗng
    if value is None or value == "":
        errors.append("so_luong: Không rỗng")
        return errors
    
    s = str(value).strip()
    
    # Rule 2: Regex - Số nguyên dương (không thập phân)
    if not ID_PATTERN.match(s):
        errors.append("so_luong: Phải là số nguyên dương (regex: ^[1-9]\\d*$)")
        return errors
    
    try:
        so_luong = int(s)
        
        # Rule 3: ≥ 1
        if so_luong < 1:
            errors.append("so_luong: Phải >= 1")
        
        # Rule 4: ≤ 1000
        if so_luong > 1000:
            errors.append("so_luong: Vượt giới hạn business (1000)")
    
    except (ValueError, TypeError):
        errors.append("so_luong: Không thể convert sang số")
    
    return errors


# ===========================================================
# NGAY DAT VALIDATION - 4 rules
# ===========================================================
def validate_ngay_dat(value) -> List[str]:
    """
    Validate ngày đặt.
    
    Rules:
    1. Không rỗng
    2. Đúng format: YYYY-MM-DD hoặc datetime
    3. Không lớn hơn ngày hiện tại
    4. Không nhỏ hơn năm 2000 (giới hạn business)
    """
    errors = []
    
    # Rule 1: Không rỗng
    if value is None or value == "":
        errors.append("ngay_dat: Không rỗng")
        return errors
    
    s = str(value).strip()
    
    # Rule 2: Regex - Kiểm tra format
    if not (DATE_ISO.match(s) or DATETIME_ISO.match(s)):
        errors.append("ngay_dat: Sai định dạng ngày (regex: YYYY-MM-DD hoặc YYYY-MM-DD HH:MM:SS)")
        return errors
    
    # Parse date
    try:
        # Nếu là datetime object
        if isinstance(value, datetime):
            ngay_dat = value
        else:
            # Parse string
            if 'T' in s or ' ' in s:
                # Có time
                ngay_dat = datetime.fromisoformat(s.replace("Z", "+00:00"))
            else:
                # Chỉ có date
                ngay_dat = datetime.strptime(s, "%Y-%m-%d")
        
        # Rule 3: Không lớn hơn ngày hiện tại
        if ngay_dat > datetime.now():
            errors.append("ngay_dat: Không được lớn hơn ngày hiện tại")
        
        # Rule 4: Không nhỏ hơn năm 2000
        if ngay_dat.year < 2000:
            errors.append("ngay_dat: Không được nhỏ hơn năm 2000")
    
    except (ValueError, AttributeError) as e:
        errors.append(f"ngay_dat: Không thể parse ngày ({e})")
    
    return errors


# ===========================================================
# TRANG THAI VALIDATION - 3 rules
# ===========================================================
def validate_trang_thai(value: Optional[str]) -> List[str]:
    """
    Validate trạng thái đơn hàng.

    - Không rỗng
    - Không ký tự đặc biệt
    - Thuộc whitelist (EN + VI)
    """
    errors = []

    if _is_blank(value):
        errors.append("trang_thai: Không rỗng")
        return errors

    s = value.strip()

    # Rule: Không ký tự đặc biệt → dùng regex riêng TRANG_THAI_CHARSET
    if not TRANG_THAI_CHARSET.match(s):
        errors.append("trang_thai: Không chứa ký tự đặc biệt")
        return errors

    # Rule: Whitelist → dùng regex riêng TRANG_THAI_DON_HANG
    if not TRANG_THAI_DON_HANG.match(s):
        errors.append("trang_thai: Giá trị không hợp lệ (không thuộc danh sách trạng thái cho phép)")

    return errors

# ===========================================================
# MAIN VALIDATION
# ===========================================================
def validate_dat_hang(
    row: dict,
    existing_ids: Optional[Set] = None,
    valid_khach_hang_ids: Optional[Set] = None,
    valid_mon_ids: Optional[Set] = None,
) -> tuple[bool, dict, List[str]]:
    """
    Validate toàn bộ row đặt hàng.
    
    Returns:
        (is_valid, fixed_row, errors)
    """
    errors: List[str] = []
    
    # Xử lý BOM trong keys nếu có
    cleaned_row = {}
    for key, value in row.items():
        clean_key = key.lstrip('\ufeff').strip()
        cleaned_row[clean_key] = value

    # Validate từng field
    if "id" in cleaned_row:
        errors.extend(validate_dat_hang_id(cleaned_row.get("id"), existing_ids))
    
    errors.extend(validate_khach_hang_id(cleaned_row.get("khach_hang_id"), valid_khach_hang_ids))
    errors.extend(validate_mon_id_fk(cleaned_row.get("mon_id"), valid_mon_ids))
    errors.extend(validate_so_luong_dat_hang(cleaned_row.get("so_luong")))
    errors.extend(validate_ngay_dat(cleaned_row.get("ngay_dat")))
    errors.extend(validate_trang_thai(cleaned_row.get("trang_thai")))

    is_valid = len(errors) == 0

    return is_valid, cleaned_row, errors
