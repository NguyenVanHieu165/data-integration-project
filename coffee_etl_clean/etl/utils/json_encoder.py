# etl/utils/json_encoder.py
import json
from datetime import date, datetime
from decimal import Decimal


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON Encoder để xử lý các kiểu dữ liệu SQL Server"""
    
    def default(self, obj):
        # Xử lý datetime và date
        if isinstance(obj, datetime):
            return obj.isoformat()
        
        if isinstance(obj, date):
            return obj.isoformat()
        
        # Xử lý Decimal
        if isinstance(obj, Decimal):
            return float(obj)
        
        # Xử lý bytes
        if isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')
        
        # Mặc định
        return super().default(obj)


def json_dumps(obj, **kwargs):
    """Helper function để dumps với custom encoder"""
    kwargs.setdefault('cls', CustomJSONEncoder)
    kwargs.setdefault('ensure_ascii', False)
    return json.dumps(obj, **kwargs)


def convert_sql_row_to_json_compatible(row):
    """
    Convert một row từ SQL Server thành dict JSON-compatible.
    
    Args:
        row: Dict từ SQL query result
    
    Returns:
        Dict với các giá trị đã convert
    """
    result = {}
    for key, value in row.items():
        if isinstance(value, (datetime, date)):
            result[key] = value.isoformat()
        elif isinstance(value, Decimal):
            result[key] = float(value)
        elif isinstance(value, bytes):
            result[key] = value.decode('utf-8', errors='ignore')
        else:
            result[key] = value
    return result
