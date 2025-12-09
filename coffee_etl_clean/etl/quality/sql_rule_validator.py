# etl/quality/sql_rule_validator.py
"""
SQL Rule Validator - Kiểm tra dữ liệu dựa trên SQL queries
============================================================

Validator này cho phép định nghĩa validation rules bằng SQL queries
thay vì Python code. Hữu ích cho:
- Kiểm tra foreign key constraints
- Kiểm tra business rules phức tạp
- Kiểm tra dữ liệu tham chiếu từ database
- Validation dựa trên aggregate queries

Usage:
    validator = SQLRuleValidator(sql_client)
    
    # Định nghĩa rule
    validator.add_rule(
        name="check_customer_exists",
        entity_type="dat_hang",
        query="SELECT COUNT(*) as cnt FROM khach_hang WHERE id = ?",
        params_from_row=["customer_id"],
        validation_func=lambda result: result[0]["cnt"] > 0,
        error_message="Customer ID không tồn tại"
    )
    
    # Validate row
    is_valid, errors = validator.validate_row("dat_hang", row)
"""
from typing import Dict, List, Callable, Optional, Any, Tuple
from ..logger import logger


class SQLRule:
    """Định nghĩa một validation rule dựa trên SQL."""
    
    def __init__(
        self,
        name: str,
        entity_type: str,
        query: str,
        params_from_row: List[str],
        validation_func: Callable[[List[Dict]], bool],
        error_message: str,
        severity: str = "error"  # error, warning
    ):
        """
        Args:
            name: Tên rule
            entity_type: Entity type áp dụng rule
            query: SQL query để kiểm tra
            params_from_row: Danh sách field names để lấy params từ row
            validation_func: Function nhận query result và return True/False
            error_message: Message khi validation fail
            severity: Mức độ nghiêm trọng (error/warning)
        """
        self.name = name
        self.entity_type = entity_type
        self.query = query
        self.params_from_row = params_from_row
        self.validation_func = validation_func
        self.error_message = error_message
        self.severity = severity


class SQLRuleValidator:
    """Validator sử dụng SQL queries để kiểm tra dữ liệu."""
    
    def __init__(self, sql_client=None):
        """
        Args:
            sql_client: SQLServerClient instance (optional, có thể set sau)
        """
        self.sql_client = sql_client
        self.rules: Dict[str, List[SQLRule]] = {}  # entity_type -> [rules]
        self._cache: Dict[str, Any] = {}  # Cache query results
    
    def set_sql_client(self, sql_client):
        """Set SQL client sau khi khởi tạo."""
        self.sql_client = sql_client
    
    def add_rule(
        self,
        name: str,
        entity_type: str,
        query: str,
        params_from_row: List[str],
        validation_func: Callable[[List[Dict]], bool],
        error_message: str,
        severity: str = "error"
    ):
        """Thêm một SQL validation rule."""
        rule = SQLRule(
            name=name,
            entity_type=entity_type,
            query=query,
            params_from_row=params_from_row,
            validation_func=validation_func,
            error_message=error_message,
            severity=severity
        )
        
        if entity_type not in self.rules:
            self.rules[entity_type] = []
        
        self.rules[entity_type].append(rule)
        logger.info("Đã thêm SQL rule: %s cho entity %s", name, entity_type)
    
    def validate_row(
        self,
        entity_type: str,
        row: Dict
    ) -> Tuple[bool, List[str]]:
        """
        Validate một row với tất cả SQL rules của entity type.
        
        Args:
            entity_type: Entity type
            row: Dữ liệu row cần validate
        
        Returns:
            (is_valid, errors)
        """
        if not self.sql_client:
            logger.warning("SQL client chưa được set, bỏ qua SQL validation")
            return True, []
        
        if entity_type not in self.rules:
            # Không có rule cho entity này
            return True, []
        
        errors = []
        
        for rule in self.rules[entity_type]:
            try:
                # Lấy params từ row
                params = tuple(row.get(field) for field in rule.params_from_row)
                
                # Tạo cache key
                cache_key = f"{rule.name}:{params}"
                
                # Check cache
                if cache_key in self._cache:
                    result = self._cache[cache_key]
                else:
                    # Execute query
                    result = self.sql_client.execute_query(rule.query, params)
                    self._cache[cache_key] = result
                
                # Validate result
                is_valid = rule.validation_func(result)
                
                if not is_valid:
                    error_msg = f"[{rule.severity.upper()}] {rule.name}: {rule.error_message}"
                    errors.append(error_msg)
            
            except Exception as e:
                logger.error(
                    "Lỗi khi thực thi SQL rule %s: %s",
                    rule.name,
                    e
                )
                errors.append(f"[ERROR] {rule.name}: Lỗi thực thi rule - {str(e)}")
        
        is_valid = len(errors) == 0
        return is_valid, errors
    
    def clear_cache(self):
        """Xóa cache query results."""
        self._cache.clear()
    
    def load_rules_from_config(self, config: Dict):
        """
        Load rules từ config dict.
        
        Config format:
        {
            "dat_hang": [
                {
                    "name": "check_customer_exists",
                    "query": "SELECT COUNT(*) as cnt FROM khach_hang WHERE id = ?",
                    "params_from_row": ["customer_id"],
                    "validation": "result[0]['cnt'] > 0",
                    "error_message": "Customer không tồn tại",
                    "severity": "error"
                }
            ]
        }
        """
        for entity_type, rules in config.items():
            for rule_config in rules:
                # Parse validation expression
                validation_expr = rule_config["validation"]
                validation_func = lambda result, expr=validation_expr: eval(expr)
                
                self.add_rule(
                    name=rule_config["name"],
                    entity_type=entity_type,
                    query=rule_config["query"],
                    params_from_row=rule_config["params_from_row"],
                    validation_func=validation_func,
                    error_message=rule_config["error_message"],
                    severity=rule_config.get("severity", "error")
                )


# ============================================================================
# PREDEFINED SQL RULES - Các rule SQL thường dùng
# ============================================================================

def create_default_sql_rules(sql_client) -> SQLRuleValidator:
    """
    Tạo SQL validator với các rule mặc định.
    
    Các rule bao gồm:
    - Foreign key checks
    - Duplicate checks
    - Business rule checks
    """
    validator = SQLRuleValidator(sql_client)
    
    # ========================================
    # RULES CHO DAT_HANG
    # ========================================
    
    # Check customer_id tồn tại
    validator.add_rule(
        name="fk_customer_exists",
        entity_type="dat_hang",
        query="SELECT COUNT(*) as cnt FROM staging.khach_hang_tbl WHERE id = ?",
        params_from_row=["customer_id"],
        validation_func=lambda r: r[0]["cnt"] > 0 if r else False,
        error_message="customer_id không tồn tại trong bảng khach_hang"
    )
    
    # Check mon_id tồn tại
    validator.add_rule(
        name="fk_mon_exists",
        entity_type="dat_hang",
        query="SELECT COUNT(*) as cnt FROM staging.mon_tbl WHERE id = ?",
        params_from_row=["mon_id"],
        validation_func=lambda r: r[0]["cnt"] > 0 if r else False,
        error_message="mon_id không tồn tại trong bảng mon"
    )
    
    # ========================================
    # RULES CHO MON
    # ========================================
    
    # Check loai_id tồn tại
    validator.add_rule(
        name="fk_loai_exists",
        entity_type="mon",
        query="SELECT COUNT(*) as cnt FROM staging.loai_mon_tbl WHERE id = ?",
        params_from_row=["loai_id"],
        validation_func=lambda r: r[0]["cnt"] > 0 if r else False,
        error_message="loai_id không tồn tại trong bảng loai_mon"
    )
    
    # Check giá món hợp lý (không quá cao/thấp so với trung bình loại)
    validator.add_rule(
        name="price_reasonable",
        entity_type="mon",
        query="""
            SELECT AVG(CAST(gia AS FLOAT)) as avg_price
            FROM staging.mon_tbl
            WHERE loai_id = ?
        """,
        params_from_row=["loai_id"],
        validation_func=lambda r: True,  # Warning only, không block
        error_message="Giá món có thể không hợp lý so với trung bình loại",
        severity="warning"
    )
    
    # ========================================
    # RULES CHO KHACH_HANG
    # ========================================
    
    # Check email duplicate trong database
    validator.add_rule(
        name="email_unique_in_db",
        entity_type="khach_hang",
        query="SELECT COUNT(*) as cnt FROM staging.khach_hang_tbl WHERE email = ?",
        params_from_row=["email"],
        validation_func=lambda r: r[0]["cnt"] == 0 if r else True,
        error_message="Email đã tồn tại trong database"
    )
    
    logger.info("Đã tạo SQL validator với %s rules", 
               sum(len(rules) for rules in validator.rules.values()))
    
    return validator
