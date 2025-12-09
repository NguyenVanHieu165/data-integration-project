"""
Data Quality Rules cho CSV tensanpham.csv
==========================================

CSV format: id, ten_san_pham, gia, loai
(Khác với mon_tbl trong DB: id, ten_mon, loai_id, gia)
"""
from typing import List, Optional, Set
from ..regex_patterns import ID_PATTERN, TEN_MON_PATTERN, POSITIVE_NUMBER, TEST_KEYWORDS,TEN_MON_PATTERNCSV


def _is_blank(value: Optional[str]) -> bool:
    """Kiểm tra giá trị rỗng."""
    return value is None or str(value).strip() == ""


# ===========================================================
# ID VALIDATION - 2 rules
# ===========================================================
def validate_mon_csv_id(raw_id, existing_ids: Optional[Set] = None) -> List[str]:
    """
    Validate ID món từ CSV.
    
    Rules:
    1. Có thể rỗng (CSV có row không có ID)
    2. Nếu có: Số nguyên dương
    """
    errors = []
    
    # CSV cho phép ID rỗng
    if _is_blank(raw_id):
        return errors  # OK, không lỗi
    
    s = str(raw_id).strip()
    
    # Rule 2: Nếu có thì phải là số nguyên dương
    if not ID_PATTERN.match(s):
        errors.append("id: Phải là số nguyên dương (regex: ^[1-9]\\d*$)")
        return errors
    
    value = int(s)
    
    return errors


# ===========================================================
# TEN SAN PHAM VALIDATION - 5 rules
# ===========================================================
def validate_ten_san_pham(value: Optional[str], existing_mon: Optional[Set] = None) -> List[str]:
    """
    Validate tên sản phẩm.
    
    Rules:
    1. Có thể rỗng (CSV có row không có tên)
    2. Nếu có: Cho phép chữ, số, khoảng trắng, - , . ( ) /
    3. Độ dài 2–200 ký tự
    4. Không từ test/fake
    5. Không trùng tên món khác (chỉ warning)
    """
    errors = []
    
    # Rule 1: Có thể rỗng (CSV có row 10 không có tên)
    if _is_blank(value):
        errors.append("ten_san_pham: Không rỗng")
        return errors
    
    s = value.strip()
    
    # Rule 2, 3: Regex - Cho phép chữ + số
    if len(s) < 2:
        errors.append("ten_san_pham: Tối thiểu 2 ký tự")
    if len(s) > 200:
        errors.append("ten_san_pham: Tối đa 200 ký tự")
    
    # Kiểm tra pattern linh hoạt hơn
    if not TEN_MON_PATTERNCSV.match(s):
        errors.append("ten_san_pham: Chỉ cho phép chữ, số, dấu (regex: ^[A-Za-zÀ-Ỵà-ỹ\\s\\-,\\.()]{2,200}$)")
    
    # Rule 4: Không test data
    if TEST_KEYWORDS.search(s):
        errors.append("ten_san_pham: Chứa từ khóa test/fake/dummy")
    
    # Rule 5: Không trùng (chỉ warning, không fail)
    # Bỏ qua vì CSV có nhiều món trùng tên (Bánh mì 1, Bánh mì 2, ...)
    
    return errors


# ===========================================================
# LOAI VALIDATION - 3 rules (Tên loại, không phải ID)
# ===========================================================
def validate_loai_name(value: Optional[str]) -> List[str]:
    """
    Validate tên loại (không phải loai_id).
    
    Rules:
    1. Có thể rỗng (CSV có row không có loại)
    2. Nếu có: Độ dài 2-100 ký tự
    3. Không test data
    """
    errors = []
    
    # Rule 1: Có thể rỗng (CSV có row 13 không có loại)
    if _is_blank(value):
        errors.append("loai: Không rỗng")
        return errors
    
    s = value.strip()
    
    # Rule 2: Độ dài
    if len(s) < 2:
        errors.append("loai: Tối thiểu 2 ký tự")
    if len(s) > 100:
        errors.append("loai: Tối đa 100 ký tự")
    
    # Rule 3: Không test data
    if TEST_KEYWORDS.search(s):
        errors.append("loai: Chứa từ khóa test/fake/dummy")
    
    return errors


# ===========================================================
# GIA VALIDATION - 4 rules
# ===========================================================
def validate_gia_csv(value) -> List[str]:
    """
    Validate giá món từ CSV.
    
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
    
    # Kiểm tra nếu là "abc" hoặc text khác
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
def validate_mon_csv(
    row: dict,
    existing_ids: Optional[Set] = None,
    existing_mon: Optional[Set] = None,
) -> tuple[bool, dict, List[str]]:
    """
    Validate toàn bộ row món ăn từ CSV (tensanpham.csv).
    
    CSV format: id, ten_san_pham, gia, loai
    
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
        errors.extend(validate_mon_csv_id(cleaned_row.get("id"), existing_ids))
    
    errors.extend(validate_ten_san_pham(cleaned_row.get("ten_san_pham"), existing_mon))
    errors.extend(validate_gia_csv(cleaned_row.get("gia")))
    errors.extend(validate_loai_name(cleaned_row.get("loai")))

    is_valid = len(errors) == 0

    return is_valid, cleaned_row, errors
