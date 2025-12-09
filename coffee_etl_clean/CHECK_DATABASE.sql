-- ============================================================================
-- SCRIPT: Kiểm tra và kết nối database
-- ============================================================================

-- 1. Liệt kê tất cả databases
SELECT 
    name AS DatabaseName,
    database_id AS ID,
    create_date AS CreatedDate,
    state_desc AS State,
    recovery_model_desc AS RecoveryModel
FROM sys.databases
WHERE name LIKE 'DB_%'
ORDER BY create_date DESC;

-- 2. Kiểm tra database có tồn tại không
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DB_20251210_000730')
BEGIN
    PRINT '✅ Database DB_20251210_000730 tồn tại'
    
    -- Kiểm tra state
    SELECT 
        name,
        state_desc,
        CASE state_desc
            WHEN 'ONLINE' THEN '✅ Database đang ONLINE'
            WHEN 'OFFLINE' THEN '❌ Database đang OFFLINE'
            WHEN 'RESTORING' THEN '⚠️ Database đang RESTORING'
            WHEN 'RECOVERING' THEN '⚠️ Database đang RECOVERING'
            WHEN 'SUSPECT' THEN '❌ Database ở trạng thái SUSPECT'
            ELSE '⚠️ Trạng thái không xác định'
        END AS Status
    FROM sys.databases
    WHERE name = 'DB_20251210_000730';
END
ELSE
BEGIN
    PRINT '❌ Database DB_20251210_000730 KHÔNG tồn tại'
END

-- 3. Nếu database OFFLINE, set ONLINE
-- Uncomment dòng dưới nếu cần
-- ALTER DATABASE DB_20251210_000730 SET ONLINE;

-- 4. Kiểm tra quyền truy cập
SELECT 
    dp.name AS UserName,
    dp.type_desc AS UserType,
    dp.create_date AS CreatedDate
FROM sys.database_principals dp
WHERE dp.type IN ('S', 'U', 'G')
ORDER BY dp.name;

-- 5. Liệt kê các database gần đây nhất
SELECT TOP 5
    name AS DatabaseName,
    create_date AS CreatedDate,
    state_desc AS State
FROM sys.databases
WHERE name LIKE 'DB_%'
ORDER BY create_date DESC;

-- ============================================================================
-- HƯỚNG DẪN SỬ DỤNG
-- ============================================================================
/*
1. Mở SQL Server Management Studio (SSMS)
2. Kết nối đến server: localhost,1433
3. Chọn database: master
4. Chạy script này
5. Xem kết quả để biết database có tồn tại không

NẾU DATABASE OFFLINE:
- Uncomment dòng: ALTER DATABASE DB_20251210_000730 SET ONLINE;
- Chạy lại script

NẾU DATABASE KHÔNG TỒN TẠI:
- Chạy lại pipeline: python RUN_ALL_STEPS.py
- Hoặc chạy STEP 4: python STEP4_TRANSFORM_LOAD.py

NẾU VẪN LỖI:
- Kiểm tra SQL Server có đang chạy không
- Kiểm tra Windows Authentication có hoạt động không
- Restart SQL Server service
*/
