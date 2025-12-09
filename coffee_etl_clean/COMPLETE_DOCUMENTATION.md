# COFFEE ETL PIPELINE - TÀI LIỆU HOÀN CHỈNH

**Phiên bản**: 2.0  
**Ngày**: 2025-12-07

---

## 📚 CẤU TRÚC TÀI LIỆU

Tài liệu được chia thành 3 phần:

1. **ARCHITECTURE_PART1.md**: Tổng quan, kiến trúc, design patterns
2. **ARCHITECTURE_PART2.md**: Cấu trúc dự án, chi tiết modules
3. **COMPLETE_DOCUMENTATION.md**: File này - Tổng hợp và bổ sung

---

## 🎯 LUỒNG XỬ LÝ CHI TIẾT

### Bước 1: Khởi tạo Pipeline

```python
class MainETLPipeline:
    def __init__(self):
        # 1. Tạo run_id theo thời gian
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 2. Tạo database name động
        self.db_name = f"DB_{self.run_id}"
        
        # 3. Khởi tạo loggers
        self.failed_logger = FailedDataLogger(self.run_id)
        self.entity_logger = EntityLogger(self.run_id)
        
        # 4. Khởi tạo stats tracking
        self.stats = {
            "produced": {},
            "consumed": {},
            "valid": {},
            "invalid": {},
            "loaded": {}
        }
```

### Bước 2: Setup Database

```python
def setup_database(self):
    # 1. Kết nối master database
    master_db = SQLServerClient(database="master")
    master_db.connect()
    
    # 2. Tạo database mới
    DatabaseManager.create_database(self.db_name, master_db)
    
    # 3. Kết nối database mới
    new_db = SQLServerClient(database=self.db_name)
    new_db.connect()
    
    # 4. Tạo schema staging
    DatabaseManager.create_staging_schema(new_db)
    
    # 5. Tạo staging tables (CSV + SQL)
    DatabaseManager.create_staging_tables(new_db)
```

### Bước 3: Producer Phase

```python
def producer_phase(self):
    with RabbitMQClient() as rabbitmq:
        # 1. Producer từ CSV
        csv_stats = self.produce_from_csv(rabbitmq)
        
        # 2. Producer từ SQL
        sql_stats = self.produce_from_sql(rabbitmq)
        
        # 3. Merge stats
        self.stats["produced"].update(csv_stats)
        self.stats["produced"].update(sql_stats)
```

**Chi tiết produce_from_csv**:
```python
def produce_from_csv(self, rabbitmq):
    csv_files = {
        "khachhang.csv": "queue_khach_hang",
        "loaisanpham.csv": "queue_loai_mon",
        "tensanpham.csv": "queue_mon",
        "nguyenlieu.csv": "queue_nguyen_lieu",
        "dathang.csv": "queue_dat_hang"
    }
    
    for file_name, queue_name in csv_files.items():
        # 1. Declare queue
        rabbitmq.declare_queue(queue_name, durable=True)
        
        # 2. Đọc CSV
        for row in csv_staging_reader(file_path):
            # 3. Tạo message
            message = {
                "source": "csv",
                "entity_type": queue_name.replace("queue_", ""),
                "data": row,
                "metadata": {
                    "file": file_name,
                    "extract_time": datetime.now().isoformat()
                }
            }
            
            # 4. Publish
            rabbitmq.publish(queue_name, message, persistent=True)
```

### Bước 4: Consumer Phase

```python
def consumer_phase(self):
    # 1. Kết nối Target DB
    self.target_db = SQLServerClient(database=self.db_name)
    self.target_db.connect()
    
    # 2. Xử lý từng queue
    queues = [
        ("queue_khach_hang", "khach_hang"),
        ("queue_loai_mon", "loai_mon"),
        ("queue_mon", "mon"),
        ("queue_nguyen_lieu", "nguyen_lieu"),
        ("queue_dat_hang", "dat_hang")
    ]
    
    for queue_name, entity_type in queues:
        self.consume_and_process(queue_name, entity_type)
```

**Chi tiết consume_and_process**:
```python
def consume_and_process(self, queue_name, entity_type):
    with RabbitMQClient() as rabbitmq:
        # 1. Kiểm tra số message
        message_count = rabbitmq.channel.queue_declare(
            queue=queue_name, passive=True
        ).method.message_count
        
        # 2. Khởi tạo tracking
        csv_valid_rows = []
        sql_valid_rows = []
        invalid_rows = []
        seen_ids = set()
        seen_emails = set()
        
        # 3. Define callback
        def callback(ch, method, properties, body):
            message = json.loads(body.decode("utf-8"))
            data = message.get("data", {})
            source = message.get("source", "unknown")
            
            # 4. Validate
            is_valid, fixed_row, errors = rule_registry.validate_row(
                entity_type=entity_type,
                row=data,
                context={
                    "existing_ids": seen_ids,
                    "existing_emails": seen_emails,
                    "source": source  # ← Quan trọng!
                }
            )
            
            # 5. Phân loại
            if is_valid:
                if source == "csv":
                    csv_valid_rows.append(fixed_row)
                else:
                    sql_valid_rows.append(fixed_row)
                
                # Track IDs
                if "id" in fixed_row:
                    seen_ids.add(int(fixed_row["id"]))
                if "email" in fixed_row:
                    seen_emails.add(fixed_row["email"].lower())
            else:
                invalid_rows.append((data, errors))
                self.failed_logger.add_failed_record(entity_type, data, errors)
                self.entity_logger.log_invalid_row(entity_type, consumed+1, errors, data)
            
            # 6. ACK
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        # 7. Consume
        rabbitmq.channel.basic_consume(queue=queue_name, on_message_callback=callback)
        while consumed < message_count:
            rabbitmq.connection.process_data_events(time_limit=1)
        
        # 8. Transform & Load
        if csv_valid_rows:
            self.transform_and_load(entity_type, csv_valid_rows, source="csv")
        if sql_valid_rows:
            self.transform_and_load(entity_type, sql_valid_rows, source="sql")
```

### Bước 5: Transform & Load

```python
def transform_and_load(self, entity_type, rows, source="csv"):
    # 1. Transform
    transformed_rows = []
    for row in rows:
        transformed_rows.append(
            DataTransformer.transform(entity_type, row)
        )
    
    # 2. Determine staging table
    suffix = "_csv" if source == "csv" else "_sql"
    staging_table = f"staging.{entity_type}{suffix}"
    
    # 3. Bulk insert
    loaded = self.target_db.bulk_insert(
        table_name=staging_table,
        data=transformed_rows,
        batch_size=1000
    )
    
    # 4. Update stats
    self.stats["loaded"][f"{entity_type}_{source}"] = loaded
```

---

## 🔍 DATA QUALITY SYSTEM

### Validation Flow

```
Row Data
    ↓
┌─────────────────────────────────────┐
│ RuleRegistry.validate_row()         │
│                                     │
│ 1. Detect source (CSV vs SQL)      │
│ 2. Route to appropriate validator  │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│ Entity-specific Validator           │
│                                     │
│ • khach_hang_rules.py               │
│ • mon_rules.py / mon_csv_rules.py   │
│ • nguyen_lieu_rules.py              │
│ • loai_mon_rules.py                 │
│ • dat_hang_rules.py                 │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│ Validation Rules (80+)              │
│                                     │
│ • Regex-based (30+ patterns)        │
│ • Business rules                    │
│ • Foreign key checks                │
│ • Duplicate detection               │
└─────────────────────────────────────┘
    ↓
(is_valid, fixed_row, errors)
```

### Validation Rules Summary

**Khách hàng (khach_hang)**: 30 rules
- ID: 8 rules
- Họ tên: 10 rules
- SĐT: 7 rules
- Email: 6 rules
- Thành phố: 5 rules

**Món ăn (mon)**: 15 rules
- ID: 2 rules
- Tên món: 5 rules
- Loại ID: 4 rules
- Giá: 4 rules

**Món ăn CSV (mon_csv)**: 12 rules
- ID: 2 rules (có thể rỗng)
- Tên sản phẩm: 5 rules
- Giá: 4 rules
- Loại (tên): 3 rules

**Nguyên liệu (nguyen_lieu)**: 17 rules
- ID: 2 rules
- Tên nguyên liệu: 5 rules
- Số lượng: 4 rules
- Đơn vị: 3 rules
- Nhà cung cấp: 3 rules

**Loại món (loai_mon)**: 12 rules
- ID: 4 rules
- Tên loại: 5 rules
- Mô tả: 3 rules

**Đặt hàng (dat_hang)**: 20 rules
- ID: 2 rules
- Khách hàng ID: 3 rules
- Món ID: 3 rules
- Số lượng: 4 rules
- Ngày đặt: 4 rules
- Trạng thái: 3 rules

**TỔNG: 106 rules**

---

## 🔄 TRANSFORMATION LOGIC

### Transform Flow

```
Raw Data (CSV/SQL)
    ↓
DataTransformer.transform(entity_type, row)
    ↓
┌─────────────────────────────────────┐
│ Detect Format                       │
│ • CSV: có ten_san_pham              │
│ • SQL: có ten_mon                   │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│ Transform Operations                │
│                                     │
│ • Type conversion                   │
│ • Field mapping                     │
│ • Data cleaning                     │
│ • Normalization                     │
└─────────────────────────────────────┘
    ↓
Transformed Data (ready for staging)
```

### Transform Examples

**Khách hàng**:
```python
# Input (CSV)
{
    "customer_id": "  123  ",
    "ho_ten": "NGUYEN VAN A",
    "sdt": "0123456789",
    "email": "Test@Gmail.COM",
    "thanh_pho": "ha noi"
}

# Output (Transformed)
{
    "customer_id": "123",
    "ho_ten": "Nguyen Van A",  # Title case
    "sdt": "0123456789",
    "email": "test@gmail.com",  # Lowercase
    "thanh_pho": "Hà Nội",
    "extract_time": datetime(2025, 12, 7, 14, 30, 22)
}
```

**Món ăn (CSV)**:
```python
# Input (CSV)
{
    "id": "1",
    "ten_san_pham": "Bánh mì",
    "gia": "25000",
    "loai": "Ăn sáng"
}

# Output (Transformed)
{
    "ten_mon": "Bánh mì",  # Mapped from ten_san_pham
    "loai_name": "Ăn sáng",  # Tên loại
    "loai_id": None,  # Cần lookup sau
    "gia": 25000.0,  # Float
    "extract_time": datetime(...)
}
```

---

## 📊 STAGING TABLES

### Schema Design

**Phân chia theo nguồn**:
- `staging.*_csv`: Dữ liệu từ CSV
- `staging.*_sql`: Dữ liệu từ SQL Server

**Lợi ích**:
1. Traceability: Biết dữ liệu từ nguồn nào
2. Data lineage: Track nguồn gốc
3. Reconciliation: So sánh 2 nguồn
4. Debugging: Dễ debug khi có vấn đề

### Table Structure

```sql
-- staging.khach_hang_csv
CREATE TABLE staging.khach_hang_csv (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id NVARCHAR(50),
    ho_ten NVARCHAR(200),
    sdt NVARCHAR(20),
    thanh_pho NVARCHAR(100),
    email NVARCHAR(200),
    source_system NVARCHAR(50),
    [file] NVARCHAR(200),
    line NVARCHAR(50),
    extract_time DATETIME,
    loaded_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);

-- staging.khach_hang_sql (cùng cấu trúc)
```

---

## 🚨 ERROR HANDLING

### Retry Mechanism

```python
@retry(times=3, delay_sec=2, label="operation")
def risky_operation():
    # Attempt 1: Try
    # Attempt 2: Wait 2s, try again
    # Attempt 3: Wait 2s, try again
    # If all fail: Raise exception
```

**Áp dụng cho**:
- Database connections
- RabbitMQ connections
- Network operations

### Transaction Management

```python
try:
    # Begin transaction (implicit)
    self.cursor.executemany(query, values)
    self.connection.commit()  # Success
except Exception as e:
    self.connection.rollback()  # Rollback on error
    raise
```

### Failed Data Logging

**2 cấp độ logging**:

1. **FailedDataLogger**: Tổng hợp tất cả
```csv
Time,Entity,Errors,Data
14:30:22,khach_hang,"email: Sai định dạng",customer_id=123|ho_ten=Test
```

2. **EntityLogger**: Chi tiết từng entity
```json
{
    "timestamp": "2025-12-07 14:30:22",
    "row_number": 5,
    "errors": ["email: Sai định dạng"],
    "data": {"customer_id": "123", "email": "invalid"}
}
```

---

## 📝 LOGGING SYSTEM

### Log Levels

```python
logger.info("Normal operation")      # INFO
logger.warning("Potential issue")    # WARNING
logger.error("Error occurred")       # ERROR
```

### Log Files

```
logs/
├── run_20251207_143022/
│   ├── failed_data.csv              # Failed records
│   ├── khach_hang_validation.log    # Entity-specific
│   └── ...
├── pipeline.log                     # General pipeline
├── data.log                         # Data processing
└── error.log                        # Errors only
```

### Log Format (JSON)

```json
{
    "time": "2025-12-07 14:30:22",
    "level": "INFO",
    "message": "Producer phase started"
}
```

---

## 🎓 BEST PRACTICES

### 1. Code Organization
- ✅ Modular design
- ✅ Single Responsibility Principle
- ✅ DRY (Don't Repeat Yourself)

### 2. Error Handling
- ✅ Try-catch ở mọi operations
- ✅ Retry cho transient errors
- ✅ Transaction management

### 3. Logging
- ✅ Structured logging (JSON)
- ✅ Multiple log levels
- ✅ Rotating logs

### 4. Testing
- ✅ Unit tests cho validators
- ✅ Integration tests cho pipeline
- ✅ Test data quality rules

### 5. Performance
- ✅ Bulk insert (batch 1000)
- ✅ Generator pattern cho CSV
- ✅ Connection pooling

---

## 📚 TÀI LIỆU THAM KHẢO

1. **ARCHITECTURE_PART1.md**: Kiến trúc, design patterns
2. **ARCHITECTURE_PART2.md**: Cấu trúc, modules
3. **README.md**: Hướng dẫn sử dụng
4. **Code comments**: Inline documentation

---

**HẾT TÀI LIỆU**
