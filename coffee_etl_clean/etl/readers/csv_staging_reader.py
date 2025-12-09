# etl/readers/csv_staging_reader.py
import csv
from typing import Iterable, Dict
from pathlib import Path


def csv_staging_reader(file_path: str) -> Iterable[Dict]:
    """Đọc file CSV staging và trả về từng dòng dưới dạng dict."""
    path = Path(file_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")
    
    if not path.is_file():
        raise ValueError(f"Đường dẫn không phải file: {file_path}")
    
    try:
        # Use utf-8-sig to handle BOM (Byte Order Mark) automatically
        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row
    except UnicodeDecodeError as e:
        raise ValueError(f"Lỗi encoding file {file_path}: {e}")
    except csv.Error as e:
        raise ValueError(f"Lỗi đọc CSV {file_path}: {e}")
