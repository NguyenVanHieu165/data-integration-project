# etl/quality/regex_patterns.py
"""
Regex Patterns - Tập trung tất cả biểu thức chính quy cho validation
"""
import re

# ==============================================
# BASIC PATTERNS
# ==============================================

# ID: Số nguyên dương (1-9 bắt đầu, theo sau là 0-9)
ID_PATTERN = re.compile(r"^[1-9]\d*$")

# Số thực dương (bao gồm 0)
POSITIVE_NUMBER = re.compile(r"^\d+(\.\d+)?$")

# Số thực (có thể âm)
NUMBER = re.compile(r"^-?\d+(\.\d+)?$")

# ==============================================
# HỌ TÊN / TÊN PATTERNS
# ==============================================

# Họ tên tiếng Việt: Chỉ chữ cái + dấu + khoảng trắng, 2-100 ký tự (tăng giới hạn)
HO_TEN_PATTERN = re.compile(r"^[A-Za-zÀ-Ỵà-ỹ\s]{2,100}$")

# Phát hiện ký tự đặc biệt không cho phép trong tên
HO_TEN_FORBIDDEN = re.compile(r"[^A-Za-zÀ-Ỵà-ỹ\s]")

# Phát hiện số trong tên
HO_TEN_HAS_DIGIT = re.compile(r"\d")

# Phát hiện khoảng trắng liên tiếp
MULTI_SPACE = re.compile(r"\s{2,}")

# Tên toàn chữ in hoa (ít nhất 2 chữ liên tiếp)
ALL_CAPS = re.compile(r"^[A-ZÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ\s]+$")

# ==============================================
# SỐ ĐIỆN THOẠI PATTERNS
# ==============================================

# SĐT Việt Nam: 9-11 chữ số
PHONE_PATTERN = re.compile(r"^[0-9]{9,11}$")

# SĐT bắt đầu bằng 0 (chuẩn VN)
PHONE_START_ZERO = re.compile(r"^0\d{8,10}$")

# SĐT di động VN (đầu số 03, 05, 07, 08, 09)
PHONE_MOBILE_VN = re.compile(r"^(03|05|07|08|09)\d{8}$")

# Phát hiện SĐT test (toàn 0 hoặc toàn 1)
PHONE_TEST_PATTERN = re.compile(r"^(0+|1+)$")

# ==============================================
# EMAIL PATTERNS
# ==============================================

# Email cơ bản
EMAIL_PATTERN = re.compile(
    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
)

# Email nâng cao (RFC 5322 simplified)
EMAIL_STRICT = re.compile(
    r"^[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*"
    r"@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9]"
    r"(?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$"
)

# Phát hiện ký tự tiếng Việt trong email (không cho phép)
EMAIL_VIET_CHAR = re.compile(r"[À-Ỵà-ỹ]")

# Email domain phổ biến
EMAIL_COMMON_DOMAINS = re.compile(
    r"@(gmail|yahoo|hotmail|outlook|icloud|protonmail)\.(com|vn|net)$",
    re.IGNORECASE
)

# Email test patterns
EMAIL_TEST_PREFIX = re.compile(r"^(test|demo|sample|example|fake|dummy)", re.IGNORECASE)

# ==============================================
# ĐỊA CHỈ / THÀNH PHỐ PATTERNS
# ==============================================

# Thành phố: Chỉ chữ + dấu TV + khoảng trắng, 2-50 ký tự
CITY_PATTERN = re.compile(r"^[A-Za-zÀ-Ỵà-ỹ\s]{2,50}$")

# Phát hiện số trong tên thành phố
CITY_HAS_DIGIT = re.compile(r"\d")

# ==============================================
# MÔN ĂN / NGUYÊN LIỆU PATTERNS
# ==============================================

# Tên món: Cho phép chữ, số, dấu, khoảng trắng, 2-200 ký tự
TEN_MON_PATTERN = re.compile(r"^[A-Za-zÀ-Ỵà-ỹ0-9\s\-,\.()]{2,200}$")
TEN_MON_PATTERNCSV = re.compile(r"^[A-Za-zÀ-Ỵà-ỹ\s\-,\.()]{2,200}$")
# Tên nguyên liệu: Tương tự tên món
TEN_NGUYEN_LIEU_PATTERN = re.compile(r"^[A-Za-zÀ-Ỵà-ỹ0-9\s\-,\.()]{2,200}$")

# Đơn vị: Chữ cái, 1-50 ký tự
DON_VI_PATTERN = re.compile(r"^[A-Za-zÀ-Ỵà-ỹ]{1,50}$")

# Mã sản phẩm: Chữ, số, gạch ngang, gạch dưới
MA_SAN_PHAM_PATTERN = re.compile(r"^[A-Za-z0-9\-_]{1,50}$")

# ==============================================
# NGÀY THÁNG PATTERNS
# ==============================================

# ISO Date: YYYY-MM-DD
DATE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# ISO DateTime: YYYY-MM-DD HH:MM:SS
DATETIME_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}")

# Date VN: DD/MM/YYYY
DATE_VN = re.compile(r"^\d{2}/\d{2}/\d{4}$")

# ==============================================
# TRẠNG THÁI / ENUM PATTERNS
# ==============================================

# Trạng thái đơn hàng (whitelist) - case insensitive
TRANG_THAI_DON_HANG = re.compile(
    r"^(Đang xử lý|Hoàn thành|Đã hủy|Chờ xác nhận|Đang giao|Đã giao"
    r"|NEW|CONFIRMED|DONE|CANCELLED)$",
    re.IGNORECASE
)

TRANG_THAI_CHARSET = re.compile(
    r"^[a-zA-ZÀ-ỹ\s]+$",
    re.IGNORECASE
)
# ==============================================
# TEST DATA PATTERNS
# ==============================================

# Phát hiện từ khóa test/fake/dummy
TEST_KEYWORDS = re.compile(
    r"(test|fake|giả|dummy|sample|demo|example)",
    re.IGNORECASE
)

# ID test (0, 123, 999, 9999)
TEST_ID_PATTERN = re.compile(r"^(0|123|999|9999)$")

# ==============================================
# HELPER FUNCTIONS
# ==============================================

def is_valid_id(value: str) -> bool:
    """Kiểm tra ID hợp lệ."""
    return bool(ID_PATTERN.match(str(value)))


def is_valid_phone(value: str) -> bool:
    """Kiểm tra SĐT hợp lệ."""
    return bool(PHONE_PATTERN.match(str(value)))


def is_valid_email(value: str) -> bool:
    """Kiểm tra email hợp lệ."""
    return bool(EMAIL_PATTERN.match(str(value)))


def is_test_data(value: str) -> bool:
    """Kiểm tra có phải test data không."""
    return bool(TEST_KEYWORDS.search(str(value)))


def has_vietnamese_chars(value: str) -> bool:
    """Kiểm tra có ký tự tiếng Việt không."""
    return bool(EMAIL_VIET_CHAR.search(str(value)))


def normalize_whitespace(value: str) -> str:
    """Chuẩn hóa khoảng trắng (loại bỏ khoảng trắng thừa)."""
    return re.sub(r"\s+", " ", str(value).strip())


def remove_non_digits(value: str) -> str:
    """Loại bỏ tất cả ký tự không phải số."""
    return re.sub(r"\D", "", str(value))


def is_valid_date_iso(value: str) -> bool:
    """Kiểm tra ngày ISO format."""
    return bool(DATE_ISO.match(str(value)) or DATETIME_ISO.match(str(value)))
