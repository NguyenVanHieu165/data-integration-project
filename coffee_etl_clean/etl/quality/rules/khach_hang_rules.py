"""
Data Quality Rules cho bảng khach_hang_tbl
==========================================

Cột: id, ho_ten, sdt, thanh_pho, email
"""
from typing import List, Optional, Set
import re
from ..regex_patterns import (
    ID_PATTERN,
    HO_TEN_PATTERN,
    HO_TEN_FORBIDDEN,
    HO_TEN_HAS_DIGIT,
    MULTI_SPACE,
    ALL_CAPS,
    PHONE_PATTERN,
    PHONE_START_ZERO,
    PHONE_TEST_PATTERN,
    EMAIL_PATTERN,
    EMAIL_VIET_CHAR,
    EMAIL_TEST_PREFIX,
    CITY_PATTERN,
    CITY_HAS_DIGIT,
    TEST_KEYWORDS
)
from ..rules_config import TEST_IDS, TEST_PHONES, VALID_CITIES


def _is_blank(value: Optional[str]) -> bool:
    """Kiểm tra giá trị rỗng."""
    return value is None or str(value).strip() == ""


def _has_repeated_chars(value: str, min_repeat: int = 3) -> bool:
    """Kiểm tra lặp ký tự >= min_repeat lần."""
    pattern = re.compile(r'(.)\1{' + str(min_repeat - 1) + r',}')
    return bool(pattern.search(value))


# ===========================================================
# ID VALIDATION - 8 rules
# ===========================================================
def validate_id(raw_id, existing_ids: Optional[Set] = None) -> List[str]:
    """
    Validate ID khách hàng.
    
    Rules:
    1. Không rỗng
    2. Số nguyên dương (regex: ^[1-9]\\d*$)
    3. Không âm (> 0)
    4. Không trùng với ID đã tồn tại
    5. Không chứa ký tự chữ
    6. Không chứa ký tự đặc biệt
    7. Không chứa khoảng trắng
    8. Không có phần thập phân
    """
    errors: List[str] = []
    
    # Xử lý BOM và strip whitespace
    s = str(raw_id).strip() if raw_id is not None else ""
    # Loại bỏ BOM nếu có
    s = s.lstrip('\ufeff')

    # Rule 1: Không rỗng
    if _is_blank(s):
        errors.append("id: Không rỗng")
        return errors

    # Rule 2, 5, 6, 7, 8: Regex - Số nguyên dương
    if not ID_PATTERN.match(s):
        errors.append("id: Phải là số nguyên dương (regex: ^[1-9]\\d*$)")
        return errors

    value = int(s)

    # Rule 3: Không âm
    if value <= 0:
        errors.append("id: Phải lớn hơn 0")
    # Kiểm tra ID test
    if value in TEST_IDS:
        errors.append("id: Không dùng ID test (0, 123, 999)")

    # Kiểm tra giới hạn INT
    if value > 2_147_483_647:
        errors.append("id: Vượt giới hạn INT (2,147,483,647)")

    return errors


# ===========================================================
# HO TEN VALIDATION - 10 rules
# ===========================================================
def validate_ho_ten(value: Optional[str]) -> List[str]:
    """
    Validate họ tên khách hàng.
    
    Rules:
    1. Không rỗng
    2. Chỉ chứa chữ cái + dấu tiếng Việt + khoảng trắng
    3. Không chứa số
    4. Không chứa ký tự đặc biệt (trừ dấu cách)
    5. Độ dài 2–50 ký tự (tăng từ 40 lên 50)
    6. Không có khoảng trắng liên tiếp
    7. Không bắt đầu/kết thúc bằng khoảng trắng
    8. Không lặp ký tự ≥3 lần
    9. Viết hoa chữ cái đầu mỗi từ (Transform)
    """
    errors = []

    # Rule 1: Không rỗng
    if _is_blank(value):
        errors.append("ho_ten: Không rỗng")
        return errors

    s = value.strip()

    # Rule 5: Độ dài 2-50 ký tự
    if len(s) < 2:
        errors.append("ho_ten: Tối thiểu 2 ký tự")
    if len(s) > 50:
        errors.append("ho_ten: Tối đa 50 ký tự")

    # Rule 3: Không chứa số
    if HO_TEN_HAS_DIGIT.search(s):
        errors.append("ho_ten: Chứa số (regex: \\d)")

    # Rule 4: Không ký tự đặc biệt (trừ dấu cách)
    if HO_TEN_FORBIDDEN.search(s):
        errors.append("ho_ten: Chứa ký tự đặc biệt không cho phép (regex: [^A-Za-zÀ-Ỵà-ỹ\\s])")

    # Rule 6: Không khoảng trắng liên tiếp
    if MULTI_SPACE.search(s):
        errors.append("ho_ten: Có khoảng trắng liên tiếp (regex: \\s{2,})")

    # Rule 8: Không lặp ký tự ≥3 lần
    if _has_repeated_chars(s, 3):
        errors.append("ho_ten: Lặp ký tự ≥3 lần (ví dụ: aaa, hhh)")

    # Kiểm tra từ khóa test (chỉ warning, không fail)
    if TEST_KEYWORDS.search(s.lower()):
        # Chỉ warning, không thêm vào errors
        pass

    return errors


# ===========================================================
# SDT VALIDATION - 7 rules
# ===========================================================
def validate_sdt(value: Optional[str]) -> List[str]:
    """
    Validate số điện thoại.
    
    Rules:
    1. Không rỗng
    2. Chỉ chứa số 0–9
    3. Độ dài 9–11 ký tự
    4. Không có ký tự chữ, ký tự đặc biệt
    5. Không phải chuỗi trùng lặp (000..., 111...)
    6. Không trùng với sdt đã có (unique)
    7. Thường bắt đầu bằng "0"
    """
    errors = []

    # Rule 1: Không rỗng
    if _is_blank(value):
        errors.append("sdt: Không rỗng")
        return errors

    s = value.strip()

    # Rule 2, 3, 4: Regex - 9-11 chữ số
    if not PHONE_PATTERN.match(s):
        errors.append("sdt: Phải gồm 9-11 chữ số (regex: ^[0-9]{9,11}$)")
        return errors

    # Rule 5: Không chuỗi trùng lặp
    if PHONE_TEST_PATTERN.match(s):
        errors.append("sdt: Không dùng số test toàn 0 hoặc toàn 1 (regex: ^(0+|1+)$)")

    # Kiểm tra SĐT test trong whitelist
    if s in TEST_PHONES:
        errors.append("sdt: Không dùng số test (000..., 111..., 123456789)")

    # Kiểm tra bắt đầu bằng 00
    if s.startswith("00"):
        errors.append("sdt: Không bắt đầu bằng 00")

    # Rule 7: Warning nếu không bắt đầu bằng 0
    if not s.startswith("0"):
        # Warning, không phải error cứng
        pass

    return errors


# ===========================================================
# THANH PHO VALIDATION - 5 rules
# ===========================================================
def validate_thanh_pho(value: Optional[str]) -> List[str]:
    """
    Validate thành phố.
    
    Rules:
    1. Không rỗng
    2. Chỉ chứa chữ cái
    3. Không số
    4. Không ký tự đặc biệt
    5. Whitelist (Hà Nội, HCM, Đà Nẵng, Hải Phòng, Huế, Cần Thơ) - case insensitive
    """
    errors = []

    # Rule 1: Không rỗng
    if _is_blank(value):
        errors.append("thanh_pho: Không rỗng")
        return errors

    s = value.strip()

    # Rule 2, 4: Regex - Chỉ chữ + dấu + space
    if not CITY_PATTERN.match(s):
        errors.append("thanh_pho: Chỉ chữ + dấu TV + khoảng trắng, 2-50 ký tự (regex: ^[A-Za-zÀ-Ỵà-ỹ\\s]{2,50}$)")

    # Rule 3: Không chứa số
    if CITY_HAS_DIGIT.search(s):
        errors.append("thanh_pho: Không chứa số (regex: \\d)")

    # Rule 5: Whitelist - case insensitive và normalize
    # Chuẩn hóa: lowercase và loại bỏ khoảng trắng thừa
    normalized = ' '.join(s.split()).lower()
    valid_cities_normalized = {city.lower() for city in VALID_CITIES}
    
    if normalized not in valid_cities_normalized:
        errors.append(f"thanh_pho: Không thuộc danh sách ({', '.join(sorted(VALID_CITIES))})")

    return errors


# ===========================================================
# EMAIL VALIDATION - 6 rules
# ===========================================================
def validate_email(value: Optional[str], existing_emails: Optional[Set] = None) -> List[str]:
    """
    Validate email.
    
    Rules:
    1. Không rỗng
    2. Đúng định dạng email chuẩn (regex)
    3. Không trùng email đã có
    4. Lowercase toàn bộ (Transform)
    5. Không chứa dấu tiếng Việt
    6. Không khoảng trắng
    """
    errors = []

    # Rule 1: Không rỗng
    if _is_blank(value):
        errors.append("email: Không rỗng")
        return errors

    s = value.strip()

    # Rule 6: Không khoảng trắng
    if " " in s:
        errors.append("email: Không chứa khoảng trắng")

    # Rule 2: Regex - Format email
    if not EMAIL_PATTERN.match(s):
        errors.append("email: Sai định dạng (regex: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$)")

    # Rule 5: Không dấu tiếng Việt
    if EMAIL_VIET_CHAR.search(s):
        errors.append("email: Chứa dấu tiếng Việt (regex: [À-Ỵà-ỹ])")

    # Kiểm tra email test - chỉ warning, không fail
    # if EMAIL_TEST_PREFIX.match(s):
    #     errors.append("email: Email test (regex: ^(test|demo|sample|example|fake|dummy))")

    # Rule 3: Không trùng lặp
    if existing_emails and s.lower() in existing_emails:
        errors.append("email: Trùng email đã tồn tại")

    return errors


# ===========================================================
# MAIN VALIDATION
# ===========================================================
def validate_khach_hang(
    row: dict,
    existing_ids: Optional[Set] = None,
    existing_emails: Optional[Set] = None,
) -> tuple[bool, dict, List[str]]:
    """
    Validate toàn bộ row khách hàng.
    
    Returns:
        (is_valid, fixed_row, errors)
    """
    errors: List[str] = []
    
    # Xử lý BOM trong keys nếu có
    cleaned_row = {}
    for key, value in row.items():
        # Loại bỏ BOM từ key
        clean_key = key.lstrip('\ufeff').strip()
        cleaned_row[clean_key] = value
    
    # Validate từng field với cleaned row
    errors.extend(validate_id(cleaned_row.get("id"), existing_ids))
    errors.extend(validate_ho_ten(cleaned_row.get("ho_ten")))
    errors.extend(validate_sdt(cleaned_row.get("sdt")))
    errors.extend(validate_thanh_pho(cleaned_row.get("thanh_pho")))
    errors.extend(validate_email(cleaned_row.get("email"), existing_emails))

    is_valid = len(errors) == 0

    return is_valid, cleaned_row, errors
