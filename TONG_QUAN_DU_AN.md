# TỔNG QUAN DỰ ÁN COFFEE ETL PIPELINE

**Phiên bản**: 2.0  
**Ngày tạo**: 2025-12-10  
**Mục đích**: Hệ thống ETL (Extract, Transform, Load) xử lý dữ liệu quán cà phê từ nhiều nguồn

---

## 📋 MỤC LỤC

1. [Giới thiệu tổng quan](#giới-thiệu)
2. [Kiến trúc hệ thống](#kiến-trúc)
3. [Luồng xử lý dữ liệu](#luồng-xử-lý)
4. [Cấu trúc thư mục](#cấu-trúc-thư-mục)
5. [Module chi tiết](#module-chi-tiết)
6. [Các bước pipeline](#các-bước-pipeline)
7. [Hệ thống Quality](#hệ-thống-quality)
8. [Dashboard & Monitoring](#dashboard)
9. [Cách sử dụng](#cách-sử-dụng)

---

## 🎯 GIỚI THIỆU TỔNG QUAN

### Mục đích dự án
Xây dựng hệ thống ETL hoàn chỉnh để:
- Thu thập dữ liệu từ 2 nguồn: **CSV files** và **SQL Server** (ComVanPhong)
- Xử lý, làm sạch và validate dữ liệu với **106 rules**
- Phân loại dữ liệu thành **CLEAN** (hợp lệ) và **ERROR** (lỗi)
- Load dữ liệu vào SQL Server staging tables
- Theo dõi và giám sát qua **Web Dashboard**

### Đặc điểm nổi bật
✅ **Kiến trúc Microservices**: Sử dụng RabbitMQ làm message broker
✅ **Data Quality**: 106 validation rules với regex patterns
✅ **Traceability**: Phân biệt nguồn dữ liệu (CSV vs SQL)
✅ **Error Handling**: Retry mechanism, transaction management
✅ **Monitoring**: Real-time dashboard với Flask
✅ **Logging**: Structured JSON logging với rotation

---

## 🏗️ KIẾN TRÚC HỆ THỐNG

### Kiến trúc tổng thể

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
├─────────────────────────────────────────────────────────────────────┤
│  📄 CSV Files (data/)          💾 SQL Server (ComVanPhong)         │
│  • khachhang.csv               • Khách hàng table                   │
│  • loaisanpham.csv             • Món ăn table                       │
│  • tensanpham.csv              • Nguyên liệu table                  │
│  • nguyenlieu.csv              • Loại món table                     │
│  • dathang.csv                 • Đặt hàng table                     │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    STEP 1: PRODUCER                                 │
│  Đọc dữ liệu từ nguồn → Gửi vào RabbitMQ queues                    │
│  • Producer CSV: CSV → RabbitMQ                                     │
│  • Producer SQL: SQL Server → RabbitMQ                              │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    RABBITMQ MESSAGE BROKER                          │
│  Queues:                                                            │
│  • queue_khach_hang    • queue_loai_mon                            │
│  • queue_mon           • queue_nguyen_lieu                          │
│  • queue_dat_hang                                                   │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    STEP 2: RAW CONSUMER                             │
│  RabbitMQ → Ghi vào staging/raw/*.csv (RAW ZONE)                   │
│  • Phân tách theo entity và source                                  │
│  • Format: entity_source_runid.csv                                  │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    STEP 3: QUALITY ENGINE                           │
│  Validate với 106 rules → Phân loại CLEAN/ERROR                    │
│  • staging/raw/*.csv → Quality Engine                               │
│  • Valid → staging/clean/*.csv                                      │
│  • Invalid → staging/error/*.csv                                    │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    STEP 4: TRANSFORM & LOAD                         │
│  Transform → Load vào SQL Server staging tables                     │
│  • staging.khach_hang_csv / staging.khach_hang_sql                 │
│  • staging.loai_mon_csv / staging.loai_mon_sql                     │
│  • staging.mon_csv / staging.mon_sql                                │
│  • staging.nguyen_lieu_csv / staging.nguyen_lieu_sql               │
│  • staging.dat_hang_csv / staging.dat_hang_sql                     │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    TARGET DATABASE                                  │
│  SQL Server: DB_YYYYMMDD_HHMMSS                                    │
│  Schema: staging                                                    │
└─────────────────────────────────────────────────────────────────────┘
```

### Design Patterns sử dụng

1. **Factory Pattern**: `DatabaseFactory` - Tạo kết nối database
2. **Registry Pattern**: `RuleRegistry` - Quản lý validation rules
3. **Strategy Pattern**: `DataTransformer` - Transform theo entity type
4. **Context Manager**: `__enter__` / `__exit__` cho resource management
5. **Retry Pattern**: Decorator `@retry` cho error handling
6. **Generator Pattern**: `csv_staging_reader` - Đọc CSV hiệu quả

---

## 🔄 LUỒNG XỬ LÝ DỮ LIỆU CHI TIẾT

### Luồng chính (Main Pipeline)

```
1. EXTRACT (Thu thập)
   ├─ CSV Reader: Đọc 5 files CSV từ thư mục data/
   └─ SQL Reader: Đọc tables từ SQL Server ComVanPhong
   
2. PRODUCE (Gửi message)
   ├─ Tạo message format: {source, entity_type, data, metadata}
   └─ Publish vào RabbitMQ queues (persistent messages)
   
3. CONSUME (Nhận message)
   ├─ Consume từ RabbitMQ queues
   └─ Ghi vào staging/raw/*.csv (RAW ZONE)
   
4. VALIDATE (Kiểm tra chất lượng)
   ├─ Đọc từ staging/raw/*.csv
   ├─ Apply 106 validation rules
   ├─ Valid → staging/clean/*.csv
   └─ Invalid → staging/error/*.csv (với error messages)
   
5. TRANSFORM (Chuẩn hóa)
   ├─ Đọc từ staging/clean/*.csv
   ├─ Type conversion, field mapping, normalization
   └─ Chuẩn bị data cho SQL Server
   
6. LOAD (Nạp dữ liệu)
   ├─ Bulk insert vào staging tables (batch 1000 rows)
   └─ Phân biệt *_csv và *_sql tables
```

### Luồng tối ưu (Pipeline Mode - Memory)

```
STEP 3 → STEP 4 (Direct Memory Transfer)
├─ STEP 3: Validate → Lưu valid data vào memory
└─ STEP 4: Nhận data từ memory → Transform → Load
   
Lợi ích:
✅ Không cần ghi/đọc file trung gian
✅ Giảm I/O operations
✅ Tăng tốc độ xử lý
✅ Giảm disk usage
```

### Message Format

```json
{
  "source": "csv",
  "entity_type": "khach_hang",
  "data": {
    "customer_id": "123",
    "ho_ten": "Nguyen Van A",
    "sdt": "0123456789",
    "email": "test@gmail.com",
    "thanh_pho": "Ha Noi"
  },
  "metadata": {
    "file": "khachhang.csv",
    "extract_time": "2025-12-10T14:30:22",
    "run_id": "20251210_143022"
  }
}
```

---

## 📁 CẤU TRÚC THƯ MỤC DỰ ÁN

```
coffee_etl_clean/
│
├── 📄 STEP1_PRODUCER.py              # Bước 1: Đọc nguồn → RabbitMQ
├── 📄 STEP2_RAW_CONSUMER.py          # Bước 2: RabbitMQ → RAW zone
├── 📄 STEP3_QUALITY_ENGINE.py        # Bước 3: Validate → CLEAN/ERROR
├── 📄 STEP4_TRANSFORM_LOAD.py        # Bước 4: Transform → SQL Server
├── 📄 RUN_ALL_STEPS.py               # Chạy toàn bộ pipeline
├── 📄 main.py                        # Pipeline tích hợp (all-in-one)
├── 📄 dashboard.py                   # Web dashboard monitoring
│
├── 📂 data/                          # Nguồn dữ liệu CSV
│   ├── khachhang.csv                 # Dữ liệu khách hàng
│   ├── loaisanpham.csv               # Loại sản phẩm
│   ├── tensanpham.csv                # Tên sản phẩm (món ăn)
│   ├── nguyenlieu.csv                # Nguyên liệu
│   └── dathang.csv                   # Đơn đặt hàng
│
├── 📂 etl/                           # Core ETL modules
│   ├── 📄 config.py                  # Cấu hình (env, database, rabbitmq)
│   ├── 📄 logger.py                  # Logging system (JSON format)
│   │
│   ├── 📂 broker/                    # RabbitMQ integration
│   │   ├── rabbitmq_client.py        # RabbitMQ client (connect, publish, consume)
│   │   ├── producer.py               # Producer logic
│   │   └── consumer.py               # Consumer logic
│   │
│   ├── 📂 db/                        # Database operations
│   │   ├── sql_client.py             # SQL Server client (CRUD, bulk insert)
│   │   ├── database_factory.py       # Factory tạo connections
│   │   └── staging_writer.py         # Ghi vào staging tables
│   │
│   ├── 📂 quality/                   # Data quality system
│   │   ├── rule_registry.py          # Registry quản lý rules
│   │   ├── regex_patterns.py         # 30+ regex patterns
│   │   ├── rules_config.py           # Cấu hình rules
│   │   │
│   │   └── 📂 rules/                 # Validation rules theo entity
│   │       ├── khach_hang_rules.py   # 30 rules cho khách hàng
│   │       ├── loai_mon_rules.py     # 12 rules cho loại món
│   │       ├── mon_rules.py          # 15 rules cho món (SQL)
│   │       ├── mon_csv_rules.py      # 12 rules cho món (CSV)
│   │       ├── nguyen_lieu_rules.py  # 17 rules cho nguyên liệu
│   │       └── dat_hang_rules.py     # 20 rules cho đặt hàng
│   │
│   ├── 📂 transformers/              # Data transformation
│   │   └── data_transformer.py       # Transform theo entity type
│   │
│   ├── 📂 readers/                   # Data readers
│   │   └── csv_staging_reader.py     # CSV reader (generator pattern)
│   │
│   └── 📂 utils/                     # Utilities
│       ├── retry.py                  # Retry decorator
│       └── json_encoder.py           # JSON encoder cho SQL types
│
├── 📂 staging/                       # Staging zones
│   ├── 📂 raw/                       # RAW zone (dữ liệu thô)
│   ├── 📂 clean/                     # CLEAN zone (dữ liệu hợp lệ)
│   └── 📂 error/                     # ERROR zone (dữ liệu lỗi)
│
├── 📂 logs/                          # Log files
│   ├── pipeline.log                  # General pipeline logs
│   ├── data.log                      # Data processing logs
│   ├── error.log                     # Error logs only
│   └── 📂 run_YYYYMMDD_HHMMSS/       # Logs theo run_id
│       ├── failed_data.csv           # Tổng hợp dữ liệu lỗi
│       └── *_validation.log          # Logs validation theo entity
│
├── 📂 templates/                     # HTML templates
│   └── dashboard.html                # Dashboard UI
│
├── 📂 sql/                           # SQL scripts
│   └── setup_staging_tables.sql      # Script tạo staging tables
│
├── 📄 .env                           # Environment variables
├── 📄 requirements.txt               # Python dependencies
└── 📄 COMPLETE_DOCUMENTATION.md      # Tài liệu đầy đủ
```

---

## 🔧 MODULE CHI TIẾT

### 1. Config Module (`etl/config.py`)

**Chức năng**: Quản lý cấu hình toàn hệ thống

```python
class Settings:
    # App settings
    APP_ENV = "development"
    LOG_LEVEL = "INFO"
    LOG_DIR = "logs"
    
    # RabbitMQ settings
    RABBITMQ_HOST = "localhost"
    RABBITMQ_PORT = 5672
    RABBITMQ_USER = "guest"
    RABBITMQ_PASSWORD = "guest"
    
    # Source Database (ComVanPhong)
    SOURCE_DB_HOST = "localhost"
    SOURCE_DB_NAME = "ComVanPhong"
    
    # Target Database (newdata)
    TARGET_DB_HOST = "localhost"
    TARGET_DB_NAME = "newdata"
```

**Đặc điểm**:
- Load từ file `.env` (python-dotenv)
- Centralized configuration
- Type conversion tự động

---

### 2. Logger Module (`etl/logger.py`)

**Chức năng**: Hệ thống logging có cấu trúc

**Log Handlers**:
1. **Console Handler**: Output ra terminal (debug)
2. **Pipeline Handler**: `pipeline.log` (TimedRotating - daily)
3. **Data Handler**: `data.log` (RotatingFile - 10MB)
4. **Error Handler**: `error.log` (WARNING+ only)

**Log Format** (JSON):
```json
{
  "time": "2025-12-10 14:30:22",
  "level": "INFO",
  "message": "Producer phase started",
  "entity": "khach_hang",
  "count": 150
}
```

**Đặc điểm**:
- Structured logging (JSON)
- Multiple handlers
- Log rotation (time & size based)
- UTF-8 encoding

---

### 3. RabbitMQ Module (`etl/broker/`)

#### `rabbitmq_client.py`

**Chức năng**: Client kết nối và tương tác với RabbitMQ

**Methods chính**:
```python
class RabbitMQClient:
    def connect()                    # Kết nối RabbitMQ
    def declare_queue()              # Khai báo queue
    def publish()                    # Gửi message
    def consume()                    # Nhận message
    def ack_message()                # Xác nhận xử lý
    def nack_message()               # Từ chối message
```

**Đặc điểm**:
- Context manager support (`with` statement)
- Retry mechanism (3 lần, delay 2s)
- Persistent messages
- Heartbeat & timeout configuration

---

### 4. Database Module (`etl/db/`)

#### `sql_client.py`

**Chức năng**: Client kết nối SQL Server

**Methods chính**:
```python
class SQLServerClient:
    def connect()                    # Kết nối SQL Server
    def execute_query()              # SELECT queries
    def execute_non_query()          # INSERT/UPDATE/DELETE
    def bulk_insert()                # Bulk insert (batch)
    def table_exists()               # Kiểm tra table
    def truncate_table()             # Xóa dữ liệu table
```

**Đặc điểm**:
- Windows Authentication support
- Transaction management (commit/rollback)
- Bulk insert với batch size
- Context manager support

#### `database_factory.py`

**Chức năng**: Factory tạo database connections

```python
class DatabaseFactory:
    @staticmethod
    def create_source_db()           # Source DB (ComVanPhong)
    
    @staticmethod
    def create_target_db()           # Target DB (newdata)

class SourceDBReader:
    def get_all_tables()             # Lấy danh sách tables
    def get_table_info()             # Thông tin table
    def read_table()                 # Đọc dữ liệu table
    def read_all_tables()            # Đọc tất cả tables
```

**Đặc điểm**:
- Auto-discovery tables
- Metadata extraction
- Flexible reading (limit, schema)

---

### 5. Quality Module (`etl/quality/`)

#### `rule_registry.py`

**Chức năng**: Registry quản lý validation rules

```python
class RuleRegistry:
    def validate_row(entity_type, row, context)
        # Validate một row theo rules của entity
        # Returns: (is_valid, fixed_row, errors)
```

**Đặc điểm**:
- Phát hiện nguồn dữ liệu (CSV vs SQL)
- Route đến validator phù hợp
- Context-aware validation (existing IDs, emails)
- Support SQL-based validation

#### `regex_patterns.py`

**Chức năng**: 30+ regex patterns cho validation

**Patterns chính**:
```python
PATTERNS = {
    "email": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    "phone_vn": r'^(0|\+84)[0-9]{9,10}$',
    "name": r'^[A-ZÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]*(\s[A-ZÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]*)*$',
    "date_iso": r'^\d{4}-\d{2}-\d{2}$',
    "positive_number": r'^\d+(\.\d+)?$',
    "vietnamese_city": r'^[A-ZÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ]',
}
```

#### Validation Rules Files

**`khach_hang_rules.py`** (30 rules):
```python
def validate_khach_hang(row, existing_ids, existing_emails):
    # ID validation (8 rules)
    # - Không rỗng, phải là số, > 0, không trùng
    
    # Họ tên validation (10 rules)
    # - Không rỗng, độ dài 2-200, format đúng, không số
    
    # SĐT validation (7 rules)
    # - Không rỗng, format VN, độ dài 10-11
    
    # Email validation (6 rules)
    # - Format đúng, domain hợp lệ, không trùng
    
    # Thành phố validation (5 rules)
    # - Không rỗng, độ dài 2-100, chữ cái đầu viết hoa
```

**`mon_rules.py`** (15 rules) - SQL format:
```python
def validate_mon(row, existing_ids, existing_mon, valid_loai_ids):
    # ID validation (2 rules)
    # Tên món validation (5 rules)
    # Loại ID validation (4 rules) - Foreign key check
    # Giá validation (4 rules)
```

**`mon_csv_rules.py`** (12 rules) - CSV format:
```python
def validate_mon_csv(row, existing_ids, existing_mon):
    # ID có thể rỗng (CSV không có ID)
    # Tên sản phẩm validation (5 rules)
    # Giá validation (4 rules)
    # Loại validation (3 rules) - Tên loại, không phải ID
```

**`nguyen_lieu_rules.py`** (17 rules):
```python
def validate_nguyen_lieu(row, existing_ids, existing_nguyen_lieu):
    # Mã nguyên liệu validation (2 rules)
    # Tên nguyên liệu validation (5 rules)
    # Số lượng validation (4 rules)
    # Đơn vị validation (3 rules)
    # Nhà cung cấp validation (3 rules)
```

**`loai_mon_rules.py`** (12 rules):
```python
def validate_loai_mon(row, existing_ids, existing_loai):
    # Mã loại validation (4 rules)
    # Tên loại validation (5 rules)
    # Mô tả validation (3 rules)
```

**`dat_hang_rules.py`** (20 rules):
```python
def validate_dat_hang(row, existing_ids, valid_khach_hang_ids, valid_mon_ids):
    # ID validation (2 rules)
    # Khách hàng ID validation (3 rules) - Foreign key
    # Món ID validation (3 rules) - Foreign key
    # Số lượng validation (4 rules)
    # Ngày đặt validation (4 rules)
    # Trạng thái validation (3 rules)
```

**Tổng cộng: 106 validation rules**

---

### 6. Transformer Module (`etl/transformers/`)

#### `data_transformer.py`

**Chức năng**: Transform dữ liệu theo entity type

**Transform operations**:
1. **Type conversion**: String → Int/Float/Date
2. **Field mapping**: CSV fields → SQL columns
3. **Data cleaning**: Trim, normalize whitespace
4. **Normalization**: Lowercase email, title case name
5. **Timestamp**: Thêm extract_time

**Ví dụ transform**:
```python
# Input (CSV)
{
    "customer_id": "  123  ",
    "ho_ten": "NGUYEN VAN A",
    "email": "Test@Gmail.COM"
}

# Output (Transformed)
{
    "customer_id": "123",
    "ho_ten": "Nguyen Van A",
    "email": "test@gmail.com",
    "extract_time": datetime(2025, 12, 10, 14, 30, 22)
}
```

---

### 7. Reader Module (`etl/readers/`)

#### `csv_staging_reader.py`

**Chức năng**: Đọc CSV files hiệu quả

**Đặc điểm**:
- Generator pattern (memory efficient)
- UTF-8-sig encoding (handle BOM)
- Error handling (encoding, CSV format)
- Yield từng row dưới dạng dict

```python
def csv_staging_reader(file_path: str) -> Iterable[Dict]:
    with open(file_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row
```

---

### 8. Utils Module (`etl/utils/`)

#### `retry.py`

**Chức năng**: Retry decorator cho error handling

```python
@retry(times=3, delay_sec=2, label="operation")
def risky_operation():
    # Attempt 1: Try
    # Attempt 2: Wait 2s, try again
    # Attempt 3: Wait 2s, try again
    # If all fail: Raise exception
```

#### `json_encoder.py`

**Chức năng**: JSON encoder cho SQL types

**Xử lý**:
- `datetime` → ISO format string
- `date` → ISO format string
- `Decimal` → float
- `bytes` → base64 string

---

## 📝 CÁC BƯỚC PIPELINE CHI TIẾT

### STEP 1: PRODUCER (`STEP1_PRODUCER.py`)

**Mục đích**: Đọc dữ liệu từ nguồn và gửi vào RabbitMQ

**Hoạt động**:
```python
class ProducerPipeline:
    def run():
        1. Kết nối RabbitMQ
        2. Producer từ CSV files
           - Đọc 5 files CSV từ data/
           - Tạo message format
           - Publish vào queues tương ứng
        3. Producer từ SQL Server
           - Auto-discovery tables
           - Đọc dữ liệu từ ComVanPhong
           - Publish vào queues
        4. In summary statistics
```

**Input**:
- CSV files: `data/*.csv`
- SQL Server: `ComVanPhong` database

**Output**:
- Messages trong RabbitMQ queues
- Stats: Số messages đã gửi

**Mapping CSV → Queue**:
```
khachhang.csv      → queue_khach_hang
loaisanpham.csv    → queue_loai_mon
tensanpham.csv     → queue_mon
nguyenlieu.csv     → queue_nguyen_lieu
dathang.csv        → queue_dat_hang
```

**Chạy standalone**:
```bash
python STEP1_PRODUCER.py
```

---

### STEP 2: RAW CONSUMER (`STEP2_RAW_CONSUMER.py`)

**Mục đích**: Consume messages từ RabbitMQ và ghi vào RAW zone

**Hoạt động**:
```python
class RawConsumerPipeline:
    def run():
        1. Tạo thư mục staging/raw/
        2. Consume từng queue
           - Đếm số messages
           - Consume tất cả messages
           - Phân loại theo entity và source
        3. Ghi vào CSV files
           - Format: entity_source_runid.csv
           - Ví dụ: khach_hang_csv_20251210_143022.csv
        4. In summary statistics
```

**Input**:
- Messages từ RabbitMQ queues

**Output**:
- CSV files trong `staging/raw/`
- Format: `{entity}_{source}_{runid}.csv`

**Ví dụ output files**:
```
staging/raw/
├── khach_hang_csv_20251210_143022.csv
├── khach_hang_sql_20251210_143022.csv
├── loai_mon_csv_20251210_143022.csv
├── mon_csv_20251210_143022.csv
└── ...
```

**Chạy standalone**:
```bash
python STEP2_RAW_CONSUMER.py
```

---

### STEP 3: QUALITY ENGINE (`STEP3_QUALITY_ENGINE.py`)

**Mục đích**: Validate dữ liệu và phân loại CLEAN/ERROR

**Hoạt động**:
```python
class QualityEnginePipeline:
    def run():
        1. Tạo thư mục staging/clean/ và staging/error/
        2. Đọc tất cả files từ staging/raw/
        3. Validate từng row với 106 rules
           - Context tracking (IDs, emails đã thấy)
           - Phát hiện duplicate
           - Check business rules
        4. Phân loại
           - Valid → staging/clean/
           - Invalid → staging/error/ (với error messages)
        5. Lưu validated data vào memory
        6. In summary statistics
```

**Input**:
- CSV files từ `staging/raw/`

**Output**:
- Valid records: `staging/clean/*.csv`
- Invalid records: `staging/error/*.csv`
- Validated data trong memory (cho STEP 4)

**Validation Policy**:
```
❌ REJECT: Dòng có BẤT KỲ lỗi nào (bao gồm cột rỗng)
✅ ACCEPT: Chỉ dòng hoàn toàn hợp lệ
```

**Error file format**:
```csv
customer_id,ho_ten,email,_errors,_row_number
123,Test,invalid,"email: Sai định dạng",5
```

**Chạy standalone**:
```bash
python STEP3_QUALITY_ENGINE.py
```

---

### STEP 4: TRANSFORM & LOAD (`STEP4_TRANSFORM_LOAD.py`)

**Mục đích**: Transform và load vào SQL Server staging tables

**Hoạt động**:
```python
class TransformLoadPipeline:
    def run(valid_data_from_memory=None):
        1. Setup database
           - Tạo database mới: DB_YYYYMMDD_HHMMSS
           - Tạo schema: staging
           - Tạo staging tables (*_csv, *_sql)
        
        2. Process data
           - Pipeline mode: Nhận data từ memory (STEP 3)
           - Standalone mode: Đọc từ staging/clean/
        
        3. Transform
           - Type conversion
           - Field mapping
           - Normalization
        
        4. Load
           - Bulk insert vào staging tables
           - Batch size: 1000 rows
           - Transaction management
        
        5. In summary statistics
```

**Input**:
- **Pipeline mode**: Validated data từ memory (STEP 3)
- **Standalone mode**: CSV files từ `staging/clean/`

**Output**:
- SQL Server database: `DB_YYYYMMDD_HHMMSS`
- Staging tables:
  ```
  staging.khach_hang_csv
  staging.khach_hang_sql
  staging.loai_mon_csv
  staging.loai_mon_sql
  staging.mon_csv
  staging.mon_sql
  staging.nguyen_lieu_csv
  staging.nguyen_lieu_sql
  staging.dat_hang_csv
  staging.dat_hang_sql
  ```

**Chạy standalone**:
```bash
python STEP4_TRANSFORM_LOAD.py [db_name]
```

---

### RUN ALL STEPS (`RUN_ALL_STEPS.py`)

**Mục đích**: Chạy toàn bộ pipeline từ đầu đến cuối

**Hoạt động**:
```python
class FullPipeline:
    def run():
        1. STEP 1: Producer
        2. Wait 2 seconds
        3. STEP 2: Raw Consumer
        4. STEP 3: Quality Engine
        5. STEP 4: Transform & Load (pipeline mode)
        6. Print final summary
```

**Đặc điểm**:
- Chạy tuần tự 4 bước
- Dùng chung run_id
- Pipeline mode: Data truyền qua memory (STEP 3 → STEP 4)
- Tự động tạo database mới

**Chạy**:
```bash
python RUN_ALL_STEPS.py
```

---

### MAIN PIPELINE (`main.py`)

**Mục đích**: Pipeline tích hợp all-in-one (không qua files)

**Hoạt động**:
```python
class MainETLPipeline:
    def run():
        PHASE 0: Setup Database
        PHASE 1: Producer → RabbitMQ
        PHASE 2: Consumer → Validate → Transform → Load
        PHASE 3: Save Failed Data
```

**Đặc điểm**:
- Tích hợp tất cả bước trong 1 process
- Không ghi files trung gian (RAW/CLEAN/ERROR)
- Validate và transform trực tiếp trong memory
- Nhanh hơn nhưng khó debug

**Chạy**:
```bash
python main.py
```

---

## 🎯 HỆ THỐNG QUALITY (106 RULES)

### Tổng quan Validation Rules

| Entity | Số Rules | Mô tả |
|--------|----------|-------|
| **khach_hang** | 30 | ID, họ tên, SĐT, email, thành phố |
| **loai_mon** | 12 | Mã loại, tên loại, mô tả |
| **mon (SQL)** | 15 | ID, tên món, loại ID, giá |
| **mon (CSV)** | 12 | Tên sản phẩm, giá, loại (tên) |
| **nguyen_lieu** | 17 | Mã, tên, số lượng, đơn vị, giá, NCC |
| **dat_hang** | 20 | ID, khách hàng ID, món ID, số lượng, ngày, trạng thái |
| **TỔNG** | **106** | |

### Chi tiết Rules theo Entity

#### 1. Khách hàng (30 rules)

**ID Validation (8 rules)**:
```
✓ Không được rỗng
✓ Phải là số nguyên
✓ Phải > 0
✓ Không được trùng (duplicate check)
✓ Độ dài hợp lý (1-10 chữ số)
✓ Không chứa ký tự đặc biệt
✓ Format đúng
✓ Trong range hợp lệ
```

**Họ tên Validation (10 rules)**:
```
✓ Không được rỗng
✓ Độ dài 2-200 ký tự
✓ Chữ cái đầu viết hoa
✓ Không chứa số
✓ Không chứa ký tự đặc biệt (trừ khoảng trắng)
✓ Format đúng (Họ Tên)
✓ Hỗ trợ tiếng Việt có dấu
✓ Không có khoảng trắng thừa
✓ Ít nhất 2 từ
✓ Mỗi từ >= 2 ký tự
```

**SĐT Validation (7 rules)**:
```
✓ Không được rỗng
✓ Chỉ chứa số
✓ Độ dài 10-11 số
✓ Bắt đầu bằng 0 hoặc +84
✓ Format VN hợp lệ
✓ Đầu số hợp lệ (03x, 05x, 07x, 08x, 09x)
✓ Không trùng lặp
```

**Email Validation (6 rules)**:
```
✓ Không được rỗng
✓ Format đúng (user@domain.com)
✓ Domain hợp lệ
✓ Không chứa ký tự đặc biệt không hợp lệ
✓ Độ dài hợp lý (5-200)
✓ Không trùng lặp (case-insensitive)
```

**Thành phố Validation (5 rules)**:
```
✓ Không được rỗng
✓ Độ dài 2-100 ký tự
✓ Chữ cái đầu viết hoa
✓ Chỉ chứa chữ cái và khoảng trắng
✓ Hỗ trợ tiếng Việt có dấu
```

#### 2. Món ăn - SQL format (15 rules)

**ID Validation (2 rules)**:
```
✓ Phải là số nguyên
✓ Phải > 0
```

**Tên món Validation (5 rules)**:
```
✓ Không được rỗng
✓ Độ dài 2-200 ký tự
✓ Không chứa ký tự đặc biệt (trừ khoảng trắng, dấu)
✓ Chữ cái đầu viết hoa
✓ Không trùng lặp
```

**Loại ID Validation (4 rules)**:
```
✓ Không được rỗng
✓ Phải là số nguyên
✓ Phải > 0
✓ Phải tồn tại trong bảng loai_mon (foreign key check)
```

**Giá Validation (4 rules)**:
```
✓ Không được rỗng
✓ Phải là số
✓ Phải > 0
✓ Trong range hợp lý (1,000 - 10,000,000 VND)
```

#### 3. Món ăn - CSV format (12 rules)

**ID Validation (2 rules)**:
```
✓ Có thể rỗng (CSV không có ID)
✓ Nếu có, phải là số nguyên > 0
```

**Tên sản phẩm Validation (5 rules)**:
```
✓ Không được rỗng
✓ Độ dài 2-200 ký tự
✓ Chữ cái đầu viết hoa
✓ Không chứa ký tự đặc biệt
✓ Không trùng lặp
```

**Giá Validation (4 rules)**:
```
✓ Không được rỗng
✓ Phải là số
✓ Phải > 0
✓ Trong range hợp lý
```

**Loại Validation (3 rules)**:
```
✓ Không được rỗng (tên loại, không phải ID)
✓ Độ dài 2-100 ký tự
✓ Format hợp lệ
```

#### 4. Nguyên liệu (17 rules)

**Mã nguyên liệu (2 rules)**:
```
✓ Không được rỗng
✓ Format hợp lệ (alphanumeric)
```

**Tên nguyên liệu (5 rules)**:
```
✓ Không được rỗng
✓ Độ dài 2-200 ký tự
✓ Chữ cái đầu viết hoa
✓ Không chứa ký tự đặc biệt
✓ Không trùng lặp
```

**Số lượng (4 rules)**:
```
✓ Không được rỗng
✓ Phải là số
✓ Phải >= 0
✓ Trong range hợp lý
```

**Đơn vị (3 rules)**:
```
✓ Không được rỗng
✓ Độ dài 1-50 ký tự
✓ Trong danh sách hợp lệ (kg, g, lít, ml, ...)
```

**Nhà cung cấp (3 rules)**:
```
✓ Không được rỗng
✓ Độ dài 2-200 ký tự
✓ Format hợp lệ
```

#### 5. Loại món (12 rules)

**Mã loại (4 rules)**:
```
✓ Không được rỗng
✓ Format hợp lệ
✓ Độ dài 1-50 ký tự
✓ Không trùng lặp
```

**Tên loại (5 rules)**:
```
✓ Không được rỗng
✓ Độ dài 2-200 ký tự
✓ Chữ cái đầu viết hoa
✓ Không chứa ký tự đặc biệt
✓ Không trùng lặp
```

**Mô tả (3 rules)**:
```
✓ Có thể rỗng
✓ Nếu có, độ dài <= 500 ký tự
✓ Format hợp lệ
```

#### 6. Đặt hàng (20 rules)

**ID (2 rules)**:
```
✓ Phải là số nguyên
✓ Phải > 0
```

**Khách hàng ID (3 rules)**:
```
✓ Không được rỗng
✓ Phải là số nguyên > 0
✓ Phải tồn tại trong bảng khach_hang (foreign key)
```

**Món ID (3 rules)**:
```
✓ Không được rỗng
✓ Phải là số nguyên > 0
✓ Phải tồn tại trong bảng mon (foreign key)
```

**Số lượng (4 rules)**:
```
✓ Không được rỗng
✓ Phải là số nguyên
✓ Phải > 0
✓ Trong range hợp lý (1-1000)
```

**Ngày đặt (4 rules)**:
```
✓ Không được rỗng
✓ Format đúng (YYYY-MM-DD)
✓ Không được trong tương lai
✓ Không quá cũ (trong vòng 5 năm)
```

**Trạng thái (3 rules)**:
```
✓ Không được rỗng
✓ Trong danh sách hợp lệ (Pending, Processing, Completed, Cancelled)
✓ Format đúng
```

### Validation Context

**Context tracking** để phát hiện duplicate và check foreign keys:

```python
context = {
    "existing_ids": set(),           # Track IDs đã thấy
    "existing_emails": set(),        # Track emails đã thấy
    "existing_mon": set(),           # Track tên món đã thấy
    "valid_loai_ids": set(),         # Valid loại IDs
    "valid_khach_hang_ids": set(),   # Valid khách hàng IDs
    "valid_mon_ids": set(),          # Valid món IDs
    "source": "csv"                  # Nguồn dữ liệu (csv/sql)
}
```

### Error Messages

**Format error messages**:
```
"id: Không được rỗng"
"email: Sai định dạng"
"sdt: Độ dài không hợp lệ (phải 10-11 số)"
"loai_id: Không tồn tại trong bảng loai_mon"
"gia: Phải > 0"
```

**Multiple errors**:
```
"id: Không được rỗng | email: Sai định dạng | sdt: Độ dài không hợp lệ"
```

---

## 📊 DASHBOARD & MONITORING

### Dashboard (`dashboard.py`)

**Mục đích**: Web interface để theo dõi pipeline

**Công nghệ**: Flask + HTML/CSS/JavaScript

**Features**:

#### 1. Zone Monitoring
```
📁 RAW Zone
   • File count: 10
   • Total records: 1,500
   • Files: khach_hang_csv_*.csv, ...

📁 CLEAN Zone
   • File count: 10
   • Total records: 1,350
   • Files: khach_hang_csv_*.csv, ...

📁 ERROR Zone
   • File count: 10
   • Total records: 150
   • Files: khach_hang_csv_*.csv, ...
```

#### 2. Entity Summary
```
Entity: khach_hang_csv
├─ RAW: 150 records
├─ CLEAN: 135 records (90%)
├─ ERROR: 15 records (10%)
└─ Status: ✅ Good

Entity: mon_sql
├─ RAW: 200 records
├─ CLEAN: 180 records (90%)
├─ ERROR: 20 records (10%)
└─ Status: ✅ Good
```

#### 3. File Viewer
- Xem nội dung files (limit 100 rows)
- Hiển thị columns và data
- Filter và search

#### 4. Pipeline Control
```
🚀 Run Pipeline
├─ STEP 1: Producer
├─ STEP 2: Raw Consumer
├─ STEP 3: Quality Engine
├─ STEP 4: Transform & Load
├─ RUN ALL: Full pipeline
└─ DIRECT: Pipeline mode (memory)
```

#### 5. Logs Viewer
```
📝 Logs
├─ Pipeline Log
├─ Data Log
└─ Error Log
```

#### 6. Statistics & Charts
- Success rate by entity
- Error rate by entity
- Records count by zone
- Timeline charts

**API Endpoints**:
```
GET  /                          # Dashboard home
GET  /api/stats                 # Zone statistics
GET  /api/entity-summary        # Entity summary
GET  /api/pipeline-info         # Pipeline info
GET  /api/file-content          # File content
GET  /api/logs                  # Logs
POST /api/run-step              # Run pipeline step
POST /api/delete-file           # Delete file
POST /api/delete-zone           # Delete zone
GET  /api/download-file         # Download file
```

**Chạy dashboard**:
```bash
python dashboard.py

# Mở browser: http://localhost:5000
```

---

## 🚀 CÁCH SỬ DỤNG

### 1. Cài đặt môi trường

**Requirements**:
```
Python 3.8+
RabbitMQ Server
SQL Server
```

**Install dependencies**:
```bash
pip install -r requirements.txt
```

**Dependencies**:
```
python-dotenv    # Environment variables
pika             # RabbitMQ client
pyodbc           # SQL Server client
flask            # Dashboard (optional)
```

### 2. Cấu hình

**File `.env`**:
```env
# App
APP_ENV=development
LOG_LEVEL=INFO
LOG_DIR=logs

# RabbitMQ
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest

# Source Database
SOURCE_DB_HOST=localhost
SOURCE_DB_PORT=1433
SOURCE_DB_NAME=ComVanPhong
SOURCE_DB_TRUSTED_CONNECTION=true
SOURCE_DB_DRIVER=ODBC Driver 17 for SQL Server

# Target Database
TARGET_DB_HOST=localhost
TARGET_DB_PORT=1433
TARGET_DB_NAME=newdata
TARGET_DB_TRUSTED_CONNECTION=true
TARGET_DB_DRIVER=ODBC Driver 17 for SQL Server
```

### 3. Chuẩn bị dữ liệu

**CSV files** trong `data/`:
```
data/
├── khachhang.csv
├── loaisanpham.csv
├── tensanpham.csv
├── nguyenlieu.csv
└── dathang.csv
```

**SQL Server**: Database `ComVanPhong` với tables

### 4. Chạy Pipeline

#### Option 1: Chạy từng bước (Recommended)

```bash
# Bước 1: Producer
python STEP1_PRODUCER.py

# Bước 2: Raw Consumer
python STEP2_RAW_CONSUMER.py

# Bước 3: Quality Engine
python STEP3_QUALITY_ENGINE.py

# Bước 4: Transform & Load
python STEP4_TRANSFORM_LOAD.py
```

**Lợi ích**:
- Dễ debug
- Có thể xem kết quả từng bước
- Linh hoạt (có thể chạy lại từng bước)

#### Option 2: Chạy toàn bộ pipeline

```bash
python RUN_ALL_STEPS.py
```

**Lợi ích**:
- Tự động chạy 4 bước
- Pipeline mode (memory transfer)
- Nhanh hơn

#### Option 3: Chạy main pipeline

```bash
python main.py
```

**Lợi ích**:
- All-in-one process
- Không ghi files trung gian
- Nhanh nhất

### 5. Theo dõi kết quả

#### Kiểm tra staging zones:
```bash
# RAW zone
ls staging/raw/

# CLEAN zone
ls staging/clean/

# ERROR zone
ls staging/error/
```

#### Kiểm tra logs:
```bash
# Pipeline log
cat logs/pipeline.log

# Data log
cat logs/data.log

# Error log
cat logs/error.log

# Run-specific logs
ls logs/run_YYYYMMDD_HHMMSS/
```

#### Kiểm tra SQL Server:
```sql
-- Kiểm tra database
SELECT name FROM sys.databases WHERE name LIKE 'DB_%'

-- Kiểm tra tables
USE DB_20251210_143022
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'staging'

-- Kiểm tra dữ liệu
SELECT COUNT(*) FROM staging.khach_hang_csv
SELECT COUNT(*) FROM staging.khach_hang_sql
```

#### Sử dụng Dashboard:
```bash
python dashboard.py

# Mở browser: http://localhost:5000
```

### 6. Xử lý lỗi

#### Xem dữ liệu lỗi:
```bash
# Error files
cat staging/error/khach_hang_csv_*.csv

# Failed data log
cat logs/run_YYYYMMDD_HHMMSS/failed_data.csv

# Validation logs
cat logs/run_YYYYMMDD_HHMMSS/khach_hang_validation.log
```

#### Sửa dữ liệu và chạy lại:
```bash
# 1. Sửa dữ liệu nguồn (CSV hoặc SQL)
# 2. Xóa staging zones
rm -rf staging/raw/* staging/clean/* staging/error/*

# 3. Chạy lại pipeline
python RUN_ALL_STEPS.py
```

### 7. Cleanup

#### Xóa staging files:
```bash
# Windows
CLEANUP_LOGS.ps1

# Hoặc manual
rm -rf staging/raw/*
rm -rf staging/clean/*
rm -rf staging/error/*
```

#### Xóa logs:
```bash
rm -rf logs/run_*
```

#### Xóa database:
```sql
DROP DATABASE DB_20251210_143022
```

---

## 📈 PERFORMANCE & OPTIMIZATION

### Tối ưu hóa đã áp dụng

1. **Generator Pattern**: CSV reader không load toàn bộ file vào memory
2. **Bulk Insert**: Insert theo batch 1000 rows
3. **Memory Transfer**: STEP 3 → STEP 4 qua memory (không qua file)
4. **Connection Pooling**: Reuse database connections
5. **Retry Mechanism**: Tự động retry khi có lỗi tạm thời
6. **Transaction Management**: Commit theo batch, rollback khi lỗi

### Metrics

**Thời gian xử lý** (ước tính):
```
1,000 records:   ~10 seconds
10,000 records:  ~1 minute
100,000 records: ~10 minutes
```

**Memory usage**:
```
Pipeline mode:    ~100-200 MB
Standalone mode:  ~50-100 MB
```

**Disk usage**:
```
RAW zone:    ~10 MB / 1000 records
CLEAN zone:  ~9 MB / 1000 records
ERROR zone:  ~1 MB / 1000 records
Logs:        ~5 MB / run
```

---

## 🔒 BEST PRACTICES

### 1. Data Quality
✅ Validate tất cả dữ liệu trước khi load
✅ Reject dòng có bất kỳ lỗi nào
✅ Log chi tiết lỗi để dễ fix
✅ Track duplicate và foreign keys

### 2. Error Handling
✅ Try-catch ở mọi operations
✅ Retry cho transient errors
✅ Transaction management
✅ Graceful degradation

### 3. Logging
✅ Structured logging (JSON)
✅ Multiple log levels
✅ Log rotation
✅ Separate error logs

### 4. Performance
✅ Bulk operations
✅ Batch processing
✅ Memory optimization
✅ Connection reuse

### 5. Monitoring
✅ Real-time dashboard
✅ Statistics tracking
✅ Alert on errors
✅ Audit trail

---

## 📚 TÀI LIỆU THAM KHẢO

1. **COMPLETE_DOCUMENTATION.md**: Tài liệu đầy đủ về kiến trúc
2. **DASHBOARD_GUIDE.md**: Hướng dẫn sử dụng dashboard
3. **Code comments**: Inline documentation trong code
4. **README.md**: Quick start guide

---

## 🎓 KẾT LUẬN

Dự án **Coffee ETL Pipeline** là một hệ thống ETL hoàn chỉnh với:

✅ **Kiến trúc rõ ràng**: 4 bước xử lý độc lập
✅ **Data Quality cao**: 106 validation rules
✅ **Traceability tốt**: Phân biệt nguồn dữ liệu
✅ **Error Handling mạnh**: Retry, transaction, logging
✅ **Monitoring đầy đủ**: Dashboard, logs, statistics
✅ **Performance tốt**: Bulk insert, memory transfer, batch processing
✅ **Maintainable**: Modular design, clean code, documentation

Hệ thống có thể mở rộng để:
- Thêm nguồn dữ liệu mới
- Thêm validation rules
- Tích hợp với Data Warehouse
- Thêm alerting và notification
- Scheduling tự động

---

**HẾT TÀI LIỆU TỔNG QUAN**
