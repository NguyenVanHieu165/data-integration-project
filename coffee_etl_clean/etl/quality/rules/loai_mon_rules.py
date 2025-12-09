
"""
Data Quality Rules cho bảng loai_mon_tbl
========================================

Cột: id, ten_loai, mo_ta
"""
from typing import List, Optional, Set
from ..regex_patterns import ID_PATTERN, TEN_MON_PATTERN, TEST_KEYWORDS


def _is_blank(value: Optional[str]) -> bool:
    """Kiểm tra giá trị rỗng."""
    return value is None or str(value).strip() == ""


# ===========================================================
# ID VALIDATION - 4 rules
# ===========================================================
def validate_loai_mon_id(raw_id, existing_ids: Optional[Set] = None) -> List[str]:
    """
    Validate ID loại món.
    
    Rules:
    1. Số nguyên dương
    2. Không trùng
    3. Không âm
    4. Không ký tự đặc biệt
    """
    errors = []
    s = str(raw_id) if raw_id is not None else ""
    
    if _is_blank(s):
        errors.append("id: Không rỗng")
        return errors
    
    # Rule 1, 4: Regex
    if not ID_PATTERN.match(s):
        errors.append("id: Phải là số nguyên dương (regex: ^[1-9]\\d*$)")
        return errors
    
    value = int(s)
    
    # Rule 3: Không âm
    if value <= 0:
        errors.append("id: Phải lớn hơn 0")
    
    return errors


# ===========================================================
# TEN LOAI VALIDATION - 5 rules
# ===========================================================
def validate_ten_loai(value: Optional[str], existing_loai: Optional[Set] = None) -> List[str]:
    """
    Validate tên loại món.
    
    Rules:
    1. Không rỗng
    2. Chỉ chứa chữ, số, khoảng trắng và ký tự - , . ( )
    3. Độ dài 2–100 ký tự
    4. Không trùng loại khác (unique)
    5. Không test data
    """
    errors = []
    
    # Rule 1: Không rỗng
    if _is_blank(value):
        errors.append("ten_loai: Không rỗng")
        return errors
    
    s = value.strip()
    
    # Rule 2, 3: Regex
    if not TEN_MON_PATTERN.match(s):
        errors.append("ten_loai: Độ dài 2-100 ký tự, cho phép chữ, số, dấu (regex: ^[A-Za-zÀ-Ỵà-ỹ0-9\\s\\-,\\.()]{2,200}$)")
    
    # Kiểm tra độ dài cụ thể
    if len(s) > 100:
        errors.append("ten_loai: Tối đa 100 ký tự")
    
    # Rule 5: Không test data
    if TEST_KEYWORDS.search(s):
        errors.append("ten_loai: Chứa từ khóa test/fake/dummy")
    
    # Rule 4: Không trùng
    if existing_loai and s.lower() in {l.lower() for l in existing_loai}:
        errors.append("ten_loai: Trùng tên loại đã tồn tại")
    
    return errors


# ===========================================================
# MO TA VALIDATION - 3 rules
# ===========================================================
def validate_mo_ta(value: Optional[str]) -> List[str]:
    """
    Validate mô tả.
    
    Rules:
    1. Có thể rỗng
    2. Nếu có: độ dài ≤ 500
    3. Nếu có: không test data
    """
    errors = []
    
    # Rule 1: Có thể rỗng
    if _is_blank(value):
        return errors
    
    s = value.strip()
    
    # Rule 2: Độ dài ≤ 500
    if len(s) > 500:
        errors.append("mo_ta: Tối đa 500 ký tự")
    
    # Rule 3: Không test data
    if TEST_KEYWORDS.search(s):
        errors.append("mo_ta: Chứa từ khóa test/fake/dummy")
    
    return errors


# ===========================================================
# MAIN VALIDATION
# ===========================================================
def validate_loai_mon(
    row: dict,
    existing_ids: Optional[Set] = None,
    existing_loai: Optional[Set] = None,
) -> tuple[bool, dict, List[str]]:
    """
    Validate toàn bộ row loại món.
    
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
        errors.extend(validate_loai_mon_id(cleaned_row.get("id"), existing_ids))
    
    errors.extend(validate_ten_loai(cleaned_row.get("ten_loai"), existing_loai))
    
    if "mo_ta" in cleaned_row:
        errors.extend(validate_mo_ta(cleaned_row.get("mo_ta")))

    is_valid = len(errors) == 0

    return is_valid, cleaned_row, errors
