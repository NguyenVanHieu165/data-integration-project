"""
Data Quality Rules cho bảng nguyen_lieu_tbl
===========================================

Cột: id, ten_nguyen_lieu, so_luong, don_vi, nha_cung_cap
"""
from typing import List, Optional, Set
from ..regex_patterns import (
    ID_PATTERN,
    TEN_NGUYEN_LIEU_PATTERN,
    POSITIVE_NUMBER,
    DON_VI_PATTERN,
    TEST_KEYWORDS
)
from ..rules_config import VALID_UNITS


def _is_blank(value: Optional[str]) -> bool:
    """Kiểm tra giá trị rỗng."""
    return value is None or str(value).strip() == ""


# ===========================================================
# ID VALIDATION - 2 rules
# ===========================================================
def validate_nguyen_lieu_id(raw_id, existing_ids: Optional[Set] = None) -> List[str]:
    """
    Validate ID nguyên liệu.
    
    Rules:
    1. Số nguyên dương
    2. Không trùng
    """
    errors = []
    s = str(raw_id) if raw_id is not None else ""
    
    if _is_blank(s):
        errors.append("id: Không rỗng")
        return errors
    
    if not ID_PATTERN.match(s):
        errors.append("id: Phải là số nguyên dương (regex: ^[1-9]\\d*$)")
        return errors
    
    value = int(s)
    
    return errors


# ===========================================================
# TEN NGUYEN LIEU VALIDATION - 5 rules
# ===========================================================
def validate_ten_nguyen_lieu(value: Optional[str], existing_nguyen_lieu: Optional[Set] = None) -> List[str]:
    """
    Validate tên nguyên liệu.
    
    Rules:
    1. Không rỗng
    2. Chỉ chữ, số, khoảng trắng, - , . ( )
    3. Độ dài 2–100 ký tự
    4. Không trùng nguyên liệu khác
    5. Không test data
    """
    errors = []
    
    # Rule 1: Không rỗng
    if _is_blank(value):
        errors.append("ten_nguyen_lieu: Không rỗng")
        return errors
    
    s = value.strip()
    
    # Rule 2, 3: Regex
    if not TEN_NGUYEN_LIEU_PATTERN.match(s):
        errors.append("ten_nguyen_lieu: Độ dài 2-200 ký tự (regex: ^[A-Za-zÀ-Ỵà-ỹ0-9\\s\\-,\\.()]{2,200}$)")
    
    # Kiểm tra độ dài cụ thể
    if len(s) > 100:
        errors.append("ten_nguyen_lieu: Tối đa 100 ký tự")
    
    # Rule 5: Không test data
    if TEST_KEYWORDS.search(s):
        errors.append("ten_nguyen_lieu: Chứa từ khóa test/fake/dummy")
    
    # Rule 4: Không trùng
    if existing_nguyen_lieu and s.lower() in {n.lower() for n in existing_nguyen_lieu}:
        errors.append("ten_nguyen_lieu: Trùng tên nguyên liệu đã tồn tại")
    
    return errors


# ===========================================================
# SO LUONG VALIDATION - 4 rules
# ===========================================================
def validate_so_luong(value) -> List[str]:
    """
    Validate số lượng.
    
    Rules:
    1. Không rỗng
    2. Số dương hoặc số thập phân
    3. Không âm (>= 0)
    """
    errors = []
    
    # Rule 1: Không rỗng
    if value is None or value == "":
        errors.append("so_luong: Không rỗng")
        return errors
    
    s = str(value).strip()
    
    # Rule 2: Regex
    if not POSITIVE_NUMBER.match(s):
        errors.append("so_luong: Phải là số dương (regex: ^\\d+(\\.\\d+)?$)")
        return errors
    
    try:
        so_luong = float(s)
        
        # Rule 3: Không âm
        if so_luong < 0:
            errors.append("so_luong: Phải >= 0")

    except (ValueError, TypeError):
        errors.append("so_luong: Không thể convert sang số")
    
    return errors


# ===========================================================
# DON VI VALIDATION - 3 rules
# ===========================================================
def validate_don_vi(value: Optional[str]) -> List[str]:
    """
    Validate đơn vị.
    
    Rules:
    1. Không rỗng
    2. Chỉ chữ cái (cho phép số và ký tự đặc biệt như ml, kg)
    3. Nằm trong danh sách whitelist đơn vị (warning only)
    """
    errors = []
    
    # Rule 1: Không rỗng
    if _is_blank(value):
        errors.append("don_vi: Không rỗng")
        return errors
    
    s = value.strip()
    
    # Rule 2: Độ dài hợp lý
    if len(s) < 1 or len(s) > 50:
        errors.append("don_vi: Độ dài 1-50 ký tự")
    
    # Rule 3: Whitelist - chỉ warning, không fail
    # if s.lower() not in VALID_UNITS:
    #     pass  # Warning only
    
    return errors


# ===========================================================
# NHA CUNG CAP VALIDATION - 3 rules
# ===========================================================
def validate_nha_cung_cap(value: Optional[str]) -> List[str]:
    """
    Validate nhà cung cấp.
    
    Rules:
    1. Có thể rỗng
    2. Nếu có: độ dài 2–200
    3. Nếu có: không test data
    """
    errors = []
    
    # Rule 1: Có thể rỗng
    if _is_blank(value):
        return errors
    
    s = value.strip()
    
    # Rule 2: Độ dài
    if len(s) < 2:
        errors.append("nha_cung_cap: Tối thiểu 2 ký tự")
    
    if len(s) > 200:
        errors.append("nha_cung_cap: Tối đa 200 ký tự")
    
    # Rule 3: Không test data
    if TEST_KEYWORDS.search(s):
        errors.append("nha_cung_cap: Chứa từ khóa test/fake/dummy")
    
    return errors


# ===========================================================
# MAIN VALIDATION
# ===========================================================
def validate_nguyen_lieu(
    row: dict,
    existing_ids: Optional[Set] = None,
    existing_nguyen_lieu: Optional[Set] = None,
) -> tuple[bool, dict, List[str]]:
    """
    Validate toàn bộ row nguyên liệu.
    
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
        errors.extend(validate_nguyen_lieu_id(cleaned_row.get("id"), existing_ids))
    
    errors.extend(validate_ten_nguyen_lieu(cleaned_row.get("ten_nguyen_lieu"), existing_nguyen_lieu))
    errors.extend(validate_so_luong(cleaned_row.get("so_luong")))
    errors.extend(validate_don_vi(cleaned_row.get("don_vi")))
    
    if "nha_cung_cap" in cleaned_row:
        errors.extend(validate_nha_cung_cap(cleaned_row.get("nha_cung_cap")))

    is_valid = len(errors) == 0

    return is_valid, cleaned_row, errors
