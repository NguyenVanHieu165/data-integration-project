-- Tạo schema staging
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
BEGIN
    EXEC('CREATE SCHEMA staging')
END
GO

-- =====================================================
-- STAGING TABLES
-- =====================================================

-- 1. Staging: Nguyên liệu
IF OBJECT_ID('staging.nguyen_lieu_tbl', 'U') IS NOT NULL
    DROP TABLE staging.nguyen_lieu_tbl;
GO

CREATE TABLE staging.nguyen_lieu_tbl (
    id INT IDENTITY(1,1) PRIMARY KEY,
    ma_nguyen_lieu NVARCHAR(50),
    ten_nguyen_lieu NVARCHAR(200),
    don_vi NVARCHAR(50),
    so_luong DECIMAL(18,2),
    gia DECIMAL(18,2),
    nha_cung_cap NVARCHAR(200),
    ngay_nhap DATETIME,
    created_at DATETIME DEFAULT GETDATE()
);
GO

-- 2. Staging: Loại món
IF OBJECT_ID('staging.loai_mon_tbl', 'U') IS NOT NULL
    DROP TABLE staging.loai_mon_tbl;
GO

CREATE TABLE staging.loai_mon_tbl (
    id INT IDENTITY(1,1) PRIMARY KEY,
    ma_loai NVARCHAR(50),
    ten_loai NVARCHAR(200),
    mo_ta NVARCHAR(500),
    created_at DATETIME DEFAULT GETDATE()
);
GO

-- 3. Staging: Khách hàng
IF OBJECT_ID('staging.khach_hang_tbl', 'U') IS NOT NULL
    DROP TABLE staging.khach_hang_tbl;
GO

CREATE TABLE staging.khach_hang_tbl (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id NVARCHAR(50),
    ho_ten NVARCHAR(200),
    sdt NVARCHAR(20),
    thanh_pho NVARCHAR(100),
    email NVARCHAR(200),
    source_system NVARCHAR(100),
    file NVARCHAR(200),
    line NVARCHAR(50),
    extract_time NVARCHAR(50),
    created_at DATETIME DEFAULT GETDATE()
);
GO

-- 4. Staging: Đặt hàng
IF OBJECT_ID('staging.dat_hang_tbl', 'U') IS NOT NULL
    DROP TABLE staging.dat_hang_tbl;
GO

CREATE TABLE staging.dat_hang_tbl (
    id INT IDENTITY(1,1) PRIMARY KEY,
    ma_don_hang NVARCHAR(50),
    customer_id NVARCHAR(50),
    ngay_dat DATETIME,
    tong_tien DECIMAL(18,2),
    trang_thai NVARCHAR(50),
    created_at DATETIME DEFAULT GETDATE()
);
GO

-- 5. Staging: Món ăn (tensanpham.csv)
IF OBJECT_ID('staging.mon_tbl', 'U') IS NOT NULL
    DROP TABLE staging.mon_tbl;
GO

CREATE TABLE staging.mon_tbl (
    id INT IDENTITY(1,1) PRIMARY KEY,
    ten_mon NVARCHAR(200),
    loai_id INT,
    gia DECIMAL(18,2),
    created_at DATETIME DEFAULT GETDATE()
);
GO

PRINT 'Đã tạo xong các staging tables!'
GO
