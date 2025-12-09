-- =====================================================
-- DATA WAREHOUSE SCHEMA
-- =====================================================

-- Tạo schema dwh
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dwh')
BEGIN
    EXEC('CREATE SCHEMA dwh')
END
GO

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

-- 1. Dim Khách hàng (SCD Type 2)
IF OBJECT_ID('dwh.dim_khach_hang', 'U') IS NOT NULL
    DROP TABLE dwh.dim_khach_hang;
GO

CREATE TABLE dwh.dim_khach_hang (
    khach_hang_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    ho_ten NVARCHAR(200),
    sdt NVARCHAR(20),
    thanh_pho NVARCHAR(100),
    email NVARCHAR(200),
    
    -- SCD Type 2 fields
    valid_from DATETIME NOT NULL DEFAULT GETDATE(),
    valid_to DATETIME NULL,
    is_current BIT NOT NULL DEFAULT 1,
    
    -- Audit fields
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);
GO

CREATE INDEX idx_customer_id ON dwh.dim_khach_hang(customer_id);
CREATE INDEX idx_is_current ON dwh.dim_khach_hang(is_current);
GO

-- 2. Dim Món ăn
IF OBJECT_ID('dwh.dim_mon', 'U') IS NOT NULL
    DROP TABLE dwh.dim_mon;
GO

CREATE TABLE dwh.dim_mon (
    mon_key INT IDENTITY(1,1) PRIMARY KEY,
    mon_id INT NOT NULL,
    ten_mon NVARCHAR(200),
    loai_id INT,
    gia DECIMAL(18,2),
    mo_ta NVARCHAR(500),
    
    -- Audit fields
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);
GO

CREATE INDEX idx_mon_id ON dwh.dim_mon(mon_id);
GO

-- 3. Dim Nguyên liệu
IF OBJECT_ID('dwh.dim_nguyen_lieu', 'U') IS NOT NULL
    DROP TABLE dwh.dim_nguyen_lieu;
GO

CREATE TABLE dwh.dim_nguyen_lieu (
    nguyen_lieu_key INT IDENTITY(1,1) PRIMARY KEY,
    nguyen_lieu_id INT NOT NULL,
    ten_nguyen_lieu NVARCHAR(200),
    don_vi NVARCHAR(50),
    nha_cung_cap NVARCHAR(200),
    
    -- Audit fields
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);
GO

CREATE INDEX idx_nguyen_lieu_id ON dwh.dim_nguyen_lieu(nguyen_lieu_id);
GO

-- 4. Dim Loại món
IF OBJECT_ID('dwh.dim_loai_mon', 'U') IS NOT NULL
    DROP TABLE dwh.dim_loai_mon;
GO

CREATE TABLE dwh.dim_loai_mon (
    loai_key INT IDENTITY(1,1) PRIMARY KEY,
    loai_id INT NOT NULL,
    ten_loai NVARCHAR(200),
    mo_ta NVARCHAR(500),
    
    -- Audit fields
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);
GO

CREATE INDEX idx_loai_id ON dwh.dim_loai_mon(loai_id);
GO

-- 5. Dim Date (Time dimension)
IF OBJECT_ID('dwh.dim_date', 'U') IS NOT NULL
    DROP TABLE dwh.dim_date;
GO

CREATE TABLE dwh.dim_date (
    date_key INT PRIMARY KEY,  -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    day_of_week INT,
    day_name NVARCHAR(20),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month INT,
    month_name NVARCHAR(20),
    quarter INT,
    year INT,
    is_weekend BIT,
    is_holiday BIT DEFAULT 0
);
GO

-- =====================================================
-- FACT TABLES
-- =====================================================

-- 1. Fact Đặt hàng
IF OBJECT_ID('dwh.fact_dat_hang', 'U') IS NOT NULL
    DROP TABLE dwh.fact_dat_hang;
GO

CREATE TABLE dwh.fact_dat_hang (
    dat_hang_key INT IDENTITY(1,1) PRIMARY KEY,
    dat_hang_id INT NOT NULL,
    
    -- Foreign keys to dimensions
    khach_hang_key INT,
    mon_key INT,
    date_key INT,
    
    -- Measures (metrics)
    so_luong INT,
    gia_don_vi DECIMAL(18,2),
    thanh_tien DECIMAL(18,2),
    
    -- Degenerate dimensions
    trang_thai NVARCHAR(50),
    ngay_dat DATETIME,
    
    -- Audit fields
    created_at DATETIME DEFAULT GETDATE(),
    
    -- Foreign key constraints
    CONSTRAINT fk_fact_khach_hang FOREIGN KEY (khach_hang_key) 
        REFERENCES dwh.dim_khach_hang(khach_hang_key),
    CONSTRAINT fk_fact_mon FOREIGN KEY (mon_key) 
        REFERENCES dwh.dim_mon(mon_key),
    CONSTRAINT fk_fact_date FOREIGN KEY (date_key) 
        REFERENCES dwh.dim_date(date_key)
);
GO

CREATE INDEX idx_fact_khach_hang ON dwh.fact_dat_hang(khach_hang_key);
CREATE INDEX idx_fact_mon ON dwh.fact_dat_hang(mon_key);
CREATE INDEX idx_fact_date ON dwh.fact_dat_hang(date_key);
CREATE INDEX idx_fact_ngay_dat ON dwh.fact_dat_hang(ngay_dat);
GO

-- 2. Fact Kho nguyên liệu (Inventory)
IF OBJECT_ID('dwh.fact_kho_nguyen_lieu', 'U') IS NOT NULL
    DROP TABLE dwh.fact_kho_nguyen_lieu;
GO

CREATE TABLE dwh.fact_kho_nguyen_lieu (
    kho_key INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Foreign keys
    nguyen_lieu_key INT,
    date_key INT,
    
    -- Measures
    so_luong_ton DECIMAL(18,2),
    so_luong_nhap DECIMAL(18,2),
    so_luong_xuat DECIMAL(18,2),
    gia_nhap DECIMAL(18,2),
    tong_gia_tri DECIMAL(18,2),
    
    -- Audit
    created_at DATETIME DEFAULT GETDATE(),
    
    CONSTRAINT fk_fact_kho_nguyen_lieu FOREIGN KEY (nguyen_lieu_key) 
        REFERENCES dwh.dim_nguyen_lieu(nguyen_lieu_key),
    CONSTRAINT fk_fact_kho_date FOREIGN KEY (date_key) 
        REFERENCES dwh.dim_date(date_key)
);
GO

CREATE INDEX idx_fact_kho_nguyen_lieu ON dwh.fact_kho_nguyen_lieu(nguyen_lieu_key);
CREATE INDEX idx_fact_kho_date ON dwh.fact_kho_nguyen_lieu(date_key);
GO

PRINT 'Đã tạo xong Data Warehouse schema!'
PRINT '  - 5 Dimension tables'
PRINT '  - 2 Fact tables'
GO
