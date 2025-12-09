"""
Data Quality Rules cho bảng mon_tbl
===================================

Cột: id, ten_mon, loai_id, gia
"""
from typing import List, Optional, Set
from ..regex_patterns import ID_PATTERN, TEN_MON_PATTERN, POSITIVE_NUMBER, TEST_KEYWORDS


def _is_blank(value: Optional[str]) -> bool:
    """Kiểm tra giá trị rỗng."""
    return value is None or str(value).strip() == ""


# ===========================================================
# ID VALIDATION - 2 rules
# ===========================================================
def validate_mon_id(raw_id, existing_ids: Optional[Set] = None) -> List[str]:
    """
    Validate ID món.
    
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
# TEN MON VALIDATION - 5 rules
# ===========================================================
def validate_ten_mon(value: Optional[str], existing_mon: Optional[Set] = None) -> List[str]:
    """
    Validate tên món.
    
    Rules:
    1. Không rỗng
    2. Cho phép chữ, số, khoảng trắng, - , . ( ) /
    3. Độ dài 2–200 ký tự
    4. Không từ test/fake
    5. Không trùng tên món khác trong menu
    """
    errors = []
    
    # Rule 1: Không rỗng
    if _is_blank(value):
        errors.append("ten_mon: Không rỗng")
        return errors
    
    s = value.strip()
    
    # Rule 2, 3: Regex
    if not TEN_MON_PATTERN.match(s):
        errors.append("ten_mon: Độ dài 2-200 ký tự, cho phép chữ, số, dấu (regex: ^[A-Za-zÀ-Ỵà-ỹ0-9\\s\\-,\\.()]{2,200}$)")
    
    # Rule 4: Không test data
    if TEST_KEYWORDS.search(s):
        errors.append("ten_mon: Chứa từ khóa test/fake/dummy")
    
    # Rule 5: Không trùng
    if existing_mon and s.lower() in {m.lower() for m in existing_mon}:
        errors.append("ten_mon: Trùng tên món đã tồn tại")
    
    return errors


# ===========================================================
# LOAI ID VALIDATION - 4 rules
# ===========================================================
def validate_loai_id(value, valid_loai_ids: Optional[Set] = None) -> List[str]:
    """
    Validate loai_id (foreign key).
    
    Rules:
    1. Không rỗng
    2. Số nguyên dương
    3. Foreign key: phải tồn tại trong loai_mon_tbl.id
    4. Trong khoảng hợp lý (1-1000)
    """
    errors = []
    
    # Rule 1: Không rỗng
    if value is None or value == "":
        errors.append("loai_id: Không rỗng")
        return errors
    
    s = str(value).strip()
    
    # Rule 2: Regex
    if not ID_PATTERN.match(s):
        errors.append("loai_id: Phải là số nguyên dương (regex: ^[1-9]\\d*$)")
        return errors
    
    try:
        loai_id = int(s)
        
        # Rule 4: Trong khoảng hợp lý
        if loai_id < 1:
            errors.append("loai_id: Phải >= 1")
        if loai_id > 1000:
            errors.append("loai_id: Vượt giới hạn hợp lý (1000)")
        
        # Rule 3: Foreign key (nếu có danh sách valid IDs)
        if valid_loai_ids and loai_id not in valid_loai_ids:
            errors.append("loai_id: Không tồn tại trong loai_mon_tbl")
    
    except (ValueError, TypeError):
        errors.append("loai_id: Không thể convert sang số")
    
    return errors


# ===========================================================
# GIA VALIDATION - 4 rules
# ===========================================================
def validate_gia(value) -> List[str]:
    """
    Validate giá món.
    
    Rules:
    1. Không rỗng
    2. Là số (int/decimal)
    3. Lớn hơn 0
    4. Không quá lớn (≤ 10,000,000)
    """
    errors = []
    
    # Rule 1: Không rỗng
    if value is None or value == "":
        errors.append("gia: Không rỗng")
        return errors
    
    s = str(value).strip()
    
    # Rule 2: Regex - Số dương
    if not POSITIVE_NUMBER.match(s):
        errors.append("gia: Phải là số dương (regex: ^\\d+(\\.\\d+)?$)")
        return errors
    
    try:
        gia = float(s)
        
        # Rule 3: Lớn hơn 0
        if gia <= 0:
            errors.append("gia: Phải > 0")
        
        # Rule 4: Không quá lớn
        if gia > 10_000_000:
            errors.append("gia: Vượt giới hạn hợp lý (10 triệu)")
    
    except (ValueError, TypeError):
        errors.append("gia: Không thể convert sang số")
    
    return errors


# ===========================================================
# MAIN VALIDATION
# ===========================================================
def validate_mon(
    row: dict,
    existing_ids: Optional[Set] = None,
    existing_mon: Optional[Set] = None,
    valid_loai_ids: Optional[Set] = None,
) -> tuple[bool, dict, List[str]]:
    """
    Validate toàn bộ row món ăn.
    
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
        errors.extend(validate_mon_id(cleaned_row.get("id"), existing_ids))
    
    errors.extend(validate_ten_mon(cleaned_row.get("ten_mon"), existing_mon))
    errors.extend(validate_loai_id(cleaned_row.get("loai_id"), valid_loai_ids))
    errors.extend(validate_gia(cleaned_row.get("gia")))

    is_valid = len(errors) == 0

    return is_valid, cleaned_row, errors
