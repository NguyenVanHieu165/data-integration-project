# etl/quality/rules_config.py
import re

# ======================
# REGEX CHO ID
# ======================
# ID: số nguyên dương, không 0, không chữ/ký tự khác
ID_REGEX = re.compile(r"^[1-9]\d*$")
# ======================
# REGEX CHO HỌ TÊN / THÀNH PHỐ
# ======================
# Độ dài tên: 2–50 ký tự (kiểm tra chung)
NAME_LENGTH_REGEX = re.compile(r"^.{2,50}$")

# Chỉ chữ + dấu tiếng Việt + khoảng trắng
NAME_CHARS_REGEX = re.compile(r"^[A-Za-zÀ-Ỵà-ỹ\s]+$")

# Bất kỳ ký tự KHÔNG phải chữ, tiếng Việt hoặc khoảng trắng
NAME_FORBIDDEN_REGEX = re.compile(r"[^A-Za-zÀ-Ỵà-ỹ\s]")

# Phát hiện khoảng trắng liên tiếp
NO_MULTI_SPACE_REGEX = re.compile(r"\s{2,}")

# Thành phố dùng chung pattern với tên
CITY_CHARS_REGEX = re.compile(r"^[A-Za-zÀ-Ỵà-ỹ\s]{2,50}$")


# ======================
# REGEX CHO SỐ ĐIỆN THOẠI
# ======================
# 9–11 chữ số, không dấu, không khoảng trắng
PHONE_REGEX = re.compile(r"^[0-9]{9,11}$")
# ======================
# REGEX CHO EMAIL
# ======================
# Email cơ bản: local@domain.tld
EMAIL_REGEX = re.compile(
    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)

# Phát hiện ký tự có dấu tiếng Việt trong email (không cho phép)
EMAIL_VIET_CHAR_REGEX = re.compile(r"[À-Ỵà-ỹ]")


# ======================
# BỘ GIÁ TRỊ TEST / FAKE / DUMMY
# ======================
# ID test không cho dùng
TEST_IDS = {0, 123, 999}

# Số điện thoại test không cho dùng
TEST_PHONES = {
    "0000000000",
    "000000000",
    "1111111111",
    "111111111",
    "123456789",
    "1234567890",
}

# Tiền tố email test
TEST_EMAIL_PREFIXES = {
    "test@",
    "example@",
    "demo@",
    "sample@",
}

# Từ khóa test / fake / dữ liệu giả
TEST_KEYWORDS = {
    "test",
    "fake",
    "giả",
    "dummy",
    "sample",
}


# ======================
# WHITELIST TỈNH/THÀNH
# ======================
VALID_CITIES = {
    "Đà Nẵng",
    "Hà Nội",
    "Hồ Chí Minh",
    "Huế",
    "Cần Thơ",
    "Hải Phòng",
}


# ======================
# WHITELIST ĐƠN VỊ
# ======================
VALID_UNITS = {
    "kg",
    "g",
    "lít",
    "lit",
    "ml",
    "l",
    "chai",
    "lọ",
    "bó",
    "gói",
    "hộp",
    "thùng",
    "cái",
    "chiếc",
    "túi",
}
