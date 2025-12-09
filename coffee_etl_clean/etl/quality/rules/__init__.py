"""
Data Quality Rules - Mỗi bảng một file
======================================

Cấu trúc:
- khach_hang_rules.py: Rules cho bảng khach_hang_tbl
- loai_mon_rules.py: Rules cho bảng loai_mon_tbl
- mon_rules.py: Rules cho bảng mon_tbl (SQL)
- mon_csv_rules.py: Rules cho CSV tensanpham.csv
- nguyen_lieu_rules.py: Rules cho bảng nguyen_lieu_tbl
- dat_hang_rules.py: Rules cho bảng dat_hang_tbl
"""

from .khach_hang_rules import validate_khach_hang
from .loai_mon_rules import validate_loai_mon
from .mon_rules import validate_mon
from .mon_csv_rules import validate_mon_csv
from .nguyen_lieu_rules import validate_nguyen_lieu
from .dat_hang_rules import validate_dat_hang

__all__ = [
    'validate_khach_hang',
    'validate_loai_mon',
    'validate_mon',
    'validate_mon_csv',
    'validate_nguyen_lieu',
    'validate_dat_hang',
]
