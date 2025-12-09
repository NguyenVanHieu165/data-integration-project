# etl/quality/queue_validator.py
"""
Queue Validator - Kiểm tra dữ liệu từ RabbitMQ queues
======================================================

Validator này kiểm tra messages từ RabbitMQ queues trước khi xử lý:
- Kiểm tra format message
- Validate dữ liệu theo rules
- Kiểm tra completeness
- Detect duplicates

Usage:
    validator = QueueValidator()
    is_valid, errors = validator.validate_message(queue_name, message)
"""
from typing import Dict, List, Tuple, Any, Optional
import json
from ..logger import logger
from .rule_registry import rule_registry


class QueueValidator:
    """Validator cho messages từ RabbitMQ queues."""
    
    # Mapping queue name → entity type
    QUEUE_ENTITY_MAP = {
        "queue_khach_hang_tbl": "khach_hang",
        "queue_nguyen_lieu_tbl": "nguyen_lieu",
        "queue_mon_tbl": "mon",
        "queue_loai_mon_tbl": "loai_mon",
        "queue_dat_hang_tbl": "dat_hang"
    }
    
    # Required fields cho từng entity
    REQUIRED_FIELDS = {
        "khach_hang": ["id", "ho_ten", "sdt", "thanh_pho", "email"],
        "nguyen_lieu": ["id", "ten_nguyen_lieu", "so_luong", "don_vi", "nha_cung_cap"],
        "mon": ["id", "ten_mon", "loai_id", "gia"],
        "loai_mon": ["id", "ten_loai"],
        "dat_hang": ["id", "khach_hang_id", "mon_id", "so_luong", "ngay_dat", "trang_thai"]
    }
    
    def __init__(self):
        self.processed_ids = {}  # Track processed IDs per queue
        self.message_count = {}  # Count messages per queue
        
        # Initialize counters
        for queue_name in self.QUEUE_ENTITY_MAP.keys():
            self.processed_ids[queue_name] = set()
            self.message_count[queue_name] = 0
    
    def validate_message(
        self,
        queue_name: str,
        message: Any,
        check_duplicates: bool = True
    ) -> Tuple[bool, Dict, List[str]]:
        """
        Validate một message từ queue.
        
        Args:
            queue_name: Tên queue (queue_khach_hang_tbl, ...)
            message: Message data (dict hoặc JSON string)
            check_duplicates: Có kiểm tra duplicate không
        
        Returns:
            (is_valid, parsed_data, errors)
        """
        errors = []
        
        # BƯỚC 1: Kiểm tra queue name hợp lệ
        if queue_name not in self.QUEUE_ENTITY_MAP:
            errors.append(f"Queue không hợp lệ: {queue_name}")
            return False, {}, errors
        
        entity_type = self.QUEUE_ENTITY_MAP[queue_name]
        
        # BƯỚC 2: Parse message
        try:
            if isinstance(message, str):
                data = json.loads(message)
            elif isinstance(message, bytes):
                data = json.loads(message.decode('utf-8'))
            elif isinstance(message, dict):
                data = message
            else:
                errors.append(f"Message format không hợp lệ: {type(message)}")
                return False, {}, errors
        except json.JSONDecodeError as e:
            errors.append(f"Lỗi parse JSON: {e}")
            return False, {}, errors
        
        # BƯỚC 3: Kiểm tra required fields
        missing_fields = self._check_required_fields(entity_type, data)
        if missing_fields:
            errors.append(f"Thiếu fields bắt buộc: {', '.join(missing_fields)}")
        
        # BƯỚC 4: Kiểm tra duplicate ID
        if check_duplicates and "id" in data:
            record_id = str(data["id"])
            if record_id in self.processed_ids[queue_name]:
                errors.append(f"Duplicate ID: {record_id} đã được xử lý trước đó")
            else:
                self.processed_ids[queue_name].add(record_id)
        
        # BƯỚC 5: Validate với business rules
        if not errors:  # Chỉ validate nếu format OK
            is_valid, fixed_data, rule_errors = rule_registry.validate_row(
                entity_type=entity_type,
                row=data,
                context={
                    "existing_ids": self.processed_ids[queue_name],
                    "enable_sql_validation": False  # Không dùng SQL validation cho queue
                }
            )
            
            if not is_valid:
                errors.extend(rule_errors)
            
            data = fixed_data
        
        # BƯỚC 6: Cập nhật counter
        self.message_count[queue_name] += 1
        
        # Kết quả
        is_valid = len(errors) == 0
        
        if not is_valid:
            logger.warning(
                "Message không hợp lệ từ queue %s: %s",
                queue_name,
                errors
            )
        
        return is_valid, data, errors
    
    def _check_required_fields(self, entity_type: str, data: Dict) -> List[str]:
        """Kiểm tra các field bắt buộc."""
        required = self.REQUIRED_FIELDS.get(entity_type, [])
        missing = []
        
        for field in required:
            if field not in data or data[field] is None or str(data[field]).strip() == "":
                missing.append(field)
        
        return missing
    
    def validate_batch(
        self,
        queue_name: str,
        messages: List[Any]
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Validate một batch messages.
        
        Args:
            queue_name: Tên queue
            messages: List of messages
        
        Returns:
            (valid_messages, invalid_messages)
        """
        valid_messages = []
        invalid_messages = []
        
        for idx, message in enumerate(messages, 1):
            is_valid, data, errors = self.validate_message(queue_name, message)
            
            if is_valid:
                valid_messages.append(data)
            else:
                invalid_messages.append({
                    "message_index": idx,
                    "data": data,
                    "errors": errors
                })
        
        logger.info(
            "Batch validation %s: %s valid, %s invalid",
            queue_name,
            len(valid_messages),
            len(invalid_messages)
        )
        
        return valid_messages, invalid_messages
    
    def get_queue_stats(self, queue_name: str) -> Dict[str, Any]:
        """Lấy thống kê cho một queue."""
        if queue_name not in self.QUEUE_ENTITY_MAP:
            return {}
        
        return {
            "queue_name": queue_name,
            "entity_type": self.QUEUE_ENTITY_MAP[queue_name],
            "total_messages": self.message_count[queue_name],
            "unique_ids": len(self.processed_ids[queue_name])
        }
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Lấy thống kê tất cả queues."""
        stats = {}
        for queue_name in self.QUEUE_ENTITY_MAP.keys():
            stats[queue_name] = self.get_queue_stats(queue_name)
        return stats
    
    def reset_queue(self, queue_name: str):
        """Reset counters cho một queue."""
        if queue_name in self.processed_ids:
            self.processed_ids[queue_name].clear()
            self.message_count[queue_name] = 0
            logger.info("Đã reset queue: %s", queue_name)
    
    def reset_all(self):
        """Reset tất cả counters."""
        for queue_name in self.QUEUE_ENTITY_MAP.keys():
            self.reset_queue(queue_name)
        logger.info("Đã reset tất cả queues")


class QueueMessageValidator:
    """
    Validator nâng cao cho queue messages với các rule bổ sung.
    """
    
    def __init__(self, base_validator: Optional[QueueValidator] = None):
        self.base_validator = base_validator or QueueValidator()
        self.custom_rules = {}  # Custom validation rules
    
    def add_custom_rule(
        self,
        queue_name: str,
        rule_name: str,
        validation_func: callable,
        error_message: str
    ):
        """
        Thêm custom validation rule cho queue.
        
        Args:
            queue_name: Tên queue
            rule_name: Tên rule
            validation_func: Function nhận data và return True/False
            error_message: Message khi validation fail
        """
        if queue_name not in self.custom_rules:
            self.custom_rules[queue_name] = []
        
        self.custom_rules[queue_name].append({
            "name": rule_name,
            "func": validation_func,
            "message": error_message
        })
        
        logger.info("Đã thêm custom rule '%s' cho queue %s", rule_name, queue_name)
    
    def validate_message(
        self,
        queue_name: str,
        message: Any
    ) -> Tuple[bool, Dict, List[str]]:
        """Validate message với base rules + custom rules."""
        
        # Base validation
        is_valid, data, errors = self.base_validator.validate_message(
            queue_name,
            message
        )
        
        # Custom rules
        if queue_name in self.custom_rules:
            for rule in self.custom_rules[queue_name]:
                try:
                    if not rule["func"](data):
                        errors.append(f"[{rule['name']}] {rule['message']}")
                        is_valid = False
                except Exception as e:
                    errors.append(f"[{rule['name']}] Lỗi thực thi rule: {e}")
                    is_valid = False
        
        return is_valid, data, errors


# ============================================================================
# PREDEFINED CUSTOM RULES - Các rule bổ sung cho queues
# ============================================================================

def create_queue_validator_with_custom_rules() -> QueueMessageValidator:
    """Tạo validator với các custom rules mặc định."""
    
    validator = QueueMessageValidator()
    
    # ========================================
    # CUSTOM RULES CHO queue_khach_hang_tbl
    # ========================================
    
    # Rule: Email phải unique (check trong batch)
    validator.add_custom_rule(
        queue_name="queue_khach_hang_tbl",
        rule_name="email_format_strict",
        validation_func=lambda data: "@" in data.get("email", "") and "." in data.get("email", ""),
        error_message="Email phải có @ và domain"
    )
    
    # Rule: Số điện thoại phải bắt đầu bằng 0
    validator.add_custom_rule(
        queue_name="queue_khach_hang_tbl",
        rule_name="phone_starts_with_zero",
        validation_func=lambda data: str(data.get("sdt", "")).startswith("0"),
        error_message="Số điện thoại phải bắt đầu bằng 0"
    )
    
    # ========================================
    # CUSTOM RULES CHO queue_mon_tbl
    # ========================================
    
    # Rule: Giá phải > 0
    validator.add_custom_rule(
        queue_name="queue_mon_tbl",
        rule_name="price_positive",
        validation_func=lambda data: int(data.get("gia", 0)) > 0,
        error_message="Giá món phải lớn hơn 0"
    )
    
    # Rule: Tên món không được quá ngắn
    validator.add_custom_rule(
        queue_name="queue_mon_tbl",
        rule_name="name_min_length",
        validation_func=lambda data: len(str(data.get("ten_mon", ""))) >= 3,
        error_message="Tên món phải có ít nhất 3 ký tự"
    )
    
    # ========================================
    # CUSTOM RULES CHO queue_nguyen_lieu_tbl
    # ========================================
    
    # Rule: Số lượng phải > 0
    validator.add_custom_rule(
        queue_name="queue_nguyen_lieu_tbl",
        rule_name="quantity_positive",
        validation_func=lambda data: float(data.get("so_luong", 0)) > 0,
        error_message="Số lượng nguyên liệu phải lớn hơn 0"
    )
    
    # ========================================
    # CUSTOM RULES CHO queue_dat_hang_tbl
    # ========================================
    
    # Rule: Số lượng đặt phải > 0
    validator.add_custom_rule(
        queue_name="queue_dat_hang_tbl",
        rule_name="order_quantity_positive",
        validation_func=lambda data: int(data.get("so_luong", 0)) > 0,
        error_message="Số lượng đặt hàng phải lớn hơn 0"
    )
    
    # Rule: Ngày đặt không được trong tương lai
    validator.add_custom_rule(
        queue_name="queue_dat_hang_tbl",
        rule_name="date_not_future",
        validation_func=lambda data: True,  # TODO: Implement date check
        error_message="Ngày đặt không được trong tương lai"
    )
    
    logger.info("Đã tạo Queue Validator với custom rules")
    
    return validator
