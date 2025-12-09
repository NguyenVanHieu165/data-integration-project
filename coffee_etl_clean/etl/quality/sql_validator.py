"""
SQL-based Validation Engine
Kiểm tra dữ liệu trực tiếp từ database bằng SQL queries
"""
from typing import Dict, List, Any, Optional
import logging
from ..db.sql_client import SQLServerClient
from .rule_registry import RuleRegistry

logger = logging.getLogger(__name__)


class SQLValidator:
    """Validate dữ liệu trực tiếp từ SQL Server"""
    
    def __init__(self, sql_client: SQLServerClient):
        self.sql_client = sql_client
        self.rule_registry = RuleRegistry()
        
    def validate_table(self, table_name: str, schema: str = "staging") -> Dict[str, Any]:
        """
        Validate toàn bộ bảng trong database
        
        Args:
            table_name: Tên bảng cần validate
            schema: Schema của bảng (staging hoặc dwh)
            
        Returns:
            Dict chứa kết quả validation
        """
        logger.info(f"Bắt đầu validate bảng {schema}.{table_name}")
        
        # Lấy rules cho bảng
        rules = self.rule_registry.get_rules(table_name)
        if not rules:
            logger.warning(f"Không tìm thấy rules cho bảng {table_name}")
            return {"valid": True, "errors": [], "total_records": 0}
        
        # Đọc dữ liệu từ database
        query = f"SELECT * FROM {schema}.{table_name}"
        records = self.sql_client.fetch_all(query)
        
        if not records:
            logger.warning(f"Bảng {schema}.{table_name} không có dữ liệu")
            return {"valid": True, "errors": [], "total_records": 0}
        
        # Validate từng record
        errors = []
        valid_count = 0
        
        for idx, record in enumerate(records, 1):
            record_errors = self._validate_record(record, rules, table_name)
            if record_errors:
                errors.extend([{
                    "record_index": idx,
                    "record_id": record.get("id"),
                    **error
                } for error in record_errors])
            else:
                valid_count += 1
        
        result = {
            "table": table_name,
            "schema": schema,
            "total_records": len(records),
            "valid_records": valid_count,
            "invalid_records": len(records) - valid_count,
            "errors": errors,
            "valid": len(errors) == 0
        }
        
        logger.info(
            f"Hoàn thành validate {table_name}: "
            f"{valid_count}/{len(records)} records hợp lệ"
        )
        
        return result
    
    def _validate_record(
        self, 
        record: Dict[str, Any], 
        rules: Dict[str, List], 
        table_name: str
    ) -> List[Dict[str, Any]]:
        """Validate một record theo rules"""
        errors = []
        
        for field, field_rules in rules.items():
            value = record.get(field)
            
            for rule in field_rules:
                try:
                    if not rule.check(value):
                        errors.append({
                            "field": field,
                            "value": value,
                            "rule": rule.__class__.__name__,
                            "message": rule.message()
                        })
                except Exception as e:
                    logger.error(
                        f"Lỗi khi validate field {field} "
                        f"với rule {rule.__class__.__name__}: {e}"
                    )
                    errors.append({
                        "field": field,
                        "value": value,
                        "rule": rule.__class__.__name__,
                        "message": f"Lỗi validation: {str(e)}"
                    })
        
        return errors
    
    def validate_all_staging_tables(self) -> Dict[str, Dict[str, Any]]:
        """Validate tất cả bảng trong staging"""
        tables = [
            "KhachHang_tbl",
            "NguyenLieu_tbl", 
            "Mon_tbl",
            "LoaiMon_tbl",
            "DatHang_tbl"
        ]
        
        results = {}
        for table in tables:
            try:
                results[table] = self.validate_table(table, schema="staging")
            except Exception as e:
                logger.error(f"Lỗi khi validate bảng {table}: {e}")
                results[table] = {
                    "valid": False,
                    "error": str(e),
                    "total_records": 0
                }
        
        return results
    
    def get_validation_summary(self, results: Dict[str, Dict[str, Any]]) -> str:
        """Tạo summary report từ kết quả validation"""
        lines = ["=" * 60, "KẾT QUẢ VALIDATION TỪ SQL", "=" * 60, ""]
        
        total_records = 0
        total_valid = 0
        total_invalid = 0
        
        for table, result in results.items():
            if "error" in result:
                lines.append(f"❌ {table}: LỖI - {result['error']}")
            else:
                total_records += result["total_records"]
                total_valid += result["valid_records"]
                total_invalid += result["invalid_records"]
                
                status = "✅" if result["valid"] else "⚠️"
                lines.append(
                    f"{status} {table}: "
                    f"{result['valid_records']}/{result['total_records']} hợp lệ"
                )
                
                if result["errors"]:
                    lines.append(f"   Số lỗi: {len(result['errors'])}")
        
        lines.extend([
            "",
            "=" * 60,
            "TỔNG KẾT:",
            f"  Tổng số records: {total_records}",
            f"  Hợp lệ: {total_valid}",
            f"  Không hợp lệ: {total_invalid}",
            "=" * 60
        ])
        
        return "\n".join(lines)
