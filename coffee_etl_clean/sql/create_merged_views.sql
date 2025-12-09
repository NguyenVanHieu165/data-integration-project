-- =====================================================
-- CREATE MERGED VIEWS
-- =====================================================
-- Views để merge dữ liệu từ CSV và SQL sources
-- Sử dụng UNION ALL để giữ tất cả records
-- =====================================================

USE [DB_YYYYMMDD_HHMMSS];  -- Thay bằng tên database thực tế
GO

-- =====================================================
-- 1. View: khach_hang_merged
-- =====================================================
IF OBJECT_ID('staging.khach_hang_merged', 'V') IS NOT NULL
    DROP VIEW staging.khach_hang_merged;
GO

CREATE VIEW staging.khach_hang_merged AS
SELECT 
    'CSV' as source,
    id,
    customer_id,
    ho_ten,
    sdt,
    thanh_pho,
    email,
    created_at
FROM staging.khach_hang_csv

UNION ALL

SELECT 
    'SQL' as source,
    id,
    customer_id,
    ho_ten,
    sdt,
    thanh_pho,
    email,
    created_at
FROM staging.khach_hang_sql;
GO

-- =====================================================
-- 2. View: loai_mon_merged
-- =====================================================
IF OBJECT_ID('staging.loai_mon_merged', 'V') IS NOT NULL
    DROP VIEW staging.loai_mon_merged;
GO

CREATE VIEW staging.loai_mon_merged AS
SELECT 
    'CSV' as source,
    id,
    ten_loai,
    mo_ta,
    created_at
FROM staging.loai_mon_csv

UNION ALL

SELECT 
    'SQL' as source,
    id,
    ten_loai,
    mo_ta,
    created_at
FROM staging.loai_mon_sql;
GO

-- =====================================================
-- 3. View: mon_merged
-- =====================================================
IF OBJECT_ID('staging.mon_merged', 'V') IS NOT NULL
    DROP VIEW staging.mon_merged;
GO

CREATE VIEW staging.mon_merged AS
SELECT 
    'CSV' as source,
    id,
    ten_mon,
    loai_id,
    gia,
    created_at
FROM staging.mon_csv

UNION ALL

SELECT 
    'SQL' as source,
    id,
    ten_mon,
    loai_id,
    gia,
    created_at
FROM staging.mon_sql;
GO

-- =====================================================
-- 4. View: nguyen_lieu_merged
-- =====================================================
IF OBJECT_ID('staging.nguyen_lieu_merged', 'V') IS NOT NULL
    DROP VIEW staging.nguyen_lieu_merged;
GO

CREATE VIEW staging.nguyen_lieu_merged AS
SELECT 
    'CSV' as source,
    id,
    ten_nguyen_lieu,
    so_luong,
    don_vi,
    nha_cung_cap,
    created_at
FROM staging.nguyen_lieu_csv

UNION ALL

SELECT 
    'SQL' as source,
    id,
    ten_nguyen_lieu,
    so_luong,
    don_vi,
    nha_cung_cap,
    created_at
FROM staging.nguyen_lieu_sql;
GO

-- =====================================================
-- 5. View: dat_hang_merged
-- =====================================================
IF OBJECT_ID('staging.dat_hang_merged', 'V') IS NOT NULL
    DROP VIEW staging.dat_hang_merged;
GO

CREATE VIEW staging.dat_hang_merged AS
SELECT 
    'CSV' as source,
    id,
    khach_hang_id,
    mon_id,
    so_luong,
    ngay_dat,
    trang_thai,
    created_at
FROM staging.dat_hang_csv

UNION ALL

SELECT 
    'SQL' as source,
    id,
    khach_hang_id,
    mon_id,
    so_luong,
    ngay_dat,
    trang_thai,
    created_at
FROM staging.dat_hang_sql;
GO

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- Kiểm tra số lượng records trong mỗi view
PRINT 'Checking merged views...';
GO

SELECT 'khach_hang_merged' as view_name, COUNT(*) as total_count,
       SUM(CASE WHEN source = 'CSV' THEN 1 ELSE 0 END) as csv_count,
       SUM(CASE WHEN source = 'SQL' THEN 1 ELSE 0 END) as sql_count
FROM staging.khach_hang_merged

UNION ALL

SELECT 'loai_mon_merged', COUNT(*),
       SUM(CASE WHEN source = 'CSV' THEN 1 ELSE 0 END),
       SUM(CASE WHEN source = 'SQL' THEN 1 ELSE 0 END)
FROM staging.loai_mon_merged

UNION ALL

SELECT 'mon_merged', COUNT(*),
       SUM(CASE WHEN source = 'CSV' THEN 1 ELSE 0 END),
       SUM(CASE WHEN source = 'SQL' THEN 1 ELSE 0 END)
FROM staging.mon_merged

UNION ALL

SELECT 'nguyen_lieu_merged', COUNT(*),
       SUM(CASE WHEN source = 'CSV' THEN 1 ELSE 0 END),
       SUM(CASE WHEN source = 'SQL' THEN 1 ELSE 0 END)
FROM staging.nguyen_lieu_merged

UNION ALL

SELECT 'dat_hang_merged', COUNT(*),
       SUM(CASE WHEN source = 'CSV' THEN 1 ELSE 0 END),
       SUM(CASE WHEN source = 'SQL' THEN 1 ELSE 0 END)
FROM staging.dat_hang_merged;
GO

-- =====================================================
-- SAMPLE QUERIES
-- =====================================================

-- Xem 10 khách hàng đầu tiên từ mỗi nguồn
SELECT TOP 10 * FROM staging.khach_hang_merged WHERE source = 'CSV' ORDER BY id;
SELECT TOP 10 * FROM staging.khach_hang_merged WHERE source = 'SQL' ORDER BY id;

-- So sánh dữ liệu trùng lặp giữa 2 nguồn (nếu có)
SELECT 
    csv.customer_id,
    csv.ho_ten as csv_ho_ten,
    sql.ho_ten as sql_ho_ten,
    csv.email as csv_email,
    sql.email as sql_email
FROM staging.khach_hang_csv csv
INNER JOIN staging.khach_hang_sql sql 
    ON csv.customer_id = sql.customer_id
WHERE csv.ho_ten <> sql.ho_ten 
   OR csv.email <> sql.email;

-- Tìm records chỉ có trong CSV
SELECT customer_id, ho_ten, email
FROM staging.khach_hang_csv
WHERE customer_id NOT IN (SELECT customer_id FROM staging.khach_hang_sql);

-- Tìm records chỉ có trong SQL
SELECT customer_id, ho_ten, email
FROM staging.khach_hang_sql
WHERE customer_id NOT IN (SELECT customer_id FROM staging.khach_hang_csv);

PRINT 'Merged views created successfully!';
GO
