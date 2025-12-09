# etl/quality/rule_registry.py
"""
Rule Registry - Đăng ký validation rules cho từng entity type
Sử dụng các file rules riêng biệt cho mỗi bảng
Hỗ trợ cả Python rules và SQL rules
"""
from typing import Dict, List, Tuple, Optional
from .rules import (
    validate_khach_hang,
    validate_loai_mon,
    validate_mon,
    validate_nguyen_lieu,
    validate_dat_hang
)
from .sql_rule_validator import SQLRuleValidator


class RuleRegistry:
    """
    Registry quản lý validation rules cho các entity types.
    
    Mỗi entity type có file rules riêng:
    - khach_hang → rules/khach_hang_rules.py
    - loai_mon → rules/loai_mon_rules.py
    - mon → rules/mon_rules.py
    - nguyen_lieu → rules/nguyen_lieu_rules.py
    - dat_hang → rules/dat_hang_rules.py
    
    Hỗ trợ thêm SQL-based validation rules.
    """
    
    def __init__(self, sql_validator: Optional[SQLRuleValidator] = None):
        self._validators = {
            "khach_hang": validate_khach_hang,
            "loai_mon": validate_loai_mon,
            "mon": validate_mon,
            "nguyen_lieu": validate_nguyen_lieu,
            "dat_hang": validate_dat_hang,
        }
        self.sql_validator = sql_validator
    
    def validate_row(
        self,
        entity_type: str,
        row: Dict,
        context: Optional[Dict] = None
    ) -> Tuple[bool, Dict, List[str]]:
        """
        Validate một row theo rules của entity type.
        Bao gồm cả Python rules và SQL rules.
        
        Args:
            entity_type: Tên entity (khach_hang, loai_mon, mon, nguyen_lieu, dat_hang)
            row: Dữ liệu cần validate
            context: Context data (existing_ids, existing_emails, valid_loai_ids, source, ...)
        
        Returns:
            (is_valid, fixed_row, errors)
        """
        context = context or {}
        
        # PHÁT HIỆN NGUỒN DỮ LIỆU: CSV hay SQL
        source = context.get("source", "unknown")
        
        # ĐẶC BIỆT: Xử lý "mon" từ CSV (tensanpham.csv)
        # CSV có: ten_san_pham, gia, loai (không có id, không có loai_id)
        # SQL có: id, ten_mon, loai_id, gia
        if entity_type == "mon" and source == "csv":
            # Validate CSV format: ten_san_pham, gia, loai
            is_valid, fixed_row, errors = self._validate_mon_csv(row, context)
        else:
            # BƯỚC 1: Python validation rules (standard)
            validator = self._validators.get(entity_type)
            
            if not validator:
                # Không có validator -> pass through
                is_valid, fixed_row, errors = False,row, []
            else:
                # Gọi validator tương ứng với context
                if entity_type == "khach_hang":
                    is_valid, fixed_row, errors = validator(
                        row,
                        existing_ids=context.get("existing_ids"),
                        existing_emails=context.get("existing_emails")
                    )
                
                elif entity_type == "loai_mon":
                    is_valid, fixed_row, errors = validator(
                        row,
                        existing_ids=context.get("existing_ids"),
                        existing_loai=context.get("existing_loai")
                    )
                
                elif entity_type == "mon":
                    is_valid, fixed_row, errors = validator(
                        row,
                        existing_ids=context.get("existing_ids"),
                        existing_mon=context.get("existing_mon"),
                        valid_loai_ids=context.get("valid_loai_ids")
                    )
                
                elif entity_type == "nguyen_lieu":
                    is_valid, fixed_row, errors = validator(
                        row,
                        existing_ids=context.get("existing_ids"),
                        existing_nguyen_lieu=context.get("existing_nguyen_lieu")
                    )
                
                elif entity_type == "dat_hang":
                    is_valid, fixed_row, errors = validator(
                        row,
                        existing_ids=context.get("existing_ids"),
                        valid_khach_hang_ids=context.get("valid_khach_hang_ids"),
                        valid_mon_ids=context.get("valid_mon_ids")
                    )
                
                else:
                    # Fallback
                    is_valid, fixed_row, errors = True, row, []
        
        # BƯỚC 2: SQL validation rules (nếu có)
        if self.sql_validator and context.get("enable_sql_validation", False):
            sql_valid, sql_errors = self.sql_validator.validate_row(
                entity_type,
                fixed_row
            )
            
            # Merge errors
            if not sql_valid:
                is_valid = False
                errors.extend(sql_errors)
        
        return is_valid, fixed_row, errors
    
    def _validate_mon_csv(
        self,
        row: Dict,
        context: Dict
    ) -> Tuple[bool, Dict, List[str]]:
        """
        Validate món từ CSV (tensanpham.csv).
        
        CSV format: id, ten_san_pham, gia, loai
        """
        from .rules.mon_csv_rules import validate_mon_csv
        
        return validate_mon_csv(
            row,
            existing_ids=context.get("existing_ids"),
            existing_mon=context.get("existing_mon")
        )


# Global registry instance
rule_registry = RuleRegistry()
