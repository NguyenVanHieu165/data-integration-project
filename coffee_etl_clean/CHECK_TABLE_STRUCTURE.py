"""
Kiểm tra cấu trúc bảng staging
"""

from etl.db.sql_client import SQLServerClient
from etl.config import settings


def check_table_structure():
    """Kiểm tra cấu trúc các bảng staging."""
    print("=" * 80)
    print("KIỂM TRA CẤU TRÚC BẢNG STAGING")
    print("=" * 80)
    
    db_name = "DB_20251209_230436"
    
    try:
        db = SQLServerClient(
            server=f"{settings.TARGET_DB_HOST},{settings.TARGET_DB_PORT}",
            database=db_name,
            driver=settings.TARGET_DB_DRIVER,
            trusted_connection=settings.TARGET_DB_TRUSTED_CONNECTION,
        )
        db.connect()
        print(f"\n✅ Connected to: {db_name}")
        
        # Kiểm tra khach_hang_csv
        print("\n" + "=" * 80)
        print("1. staging.khach_hang_csv")
        print("=" * 80)
        
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'staging'
        AND TABLE_NAME = 'khach_hang_csv'
        ORDER BY ORDINAL_POSITION
        """
        
        columns = db.execute_query(query)
        for col in columns:
            nullable = "NULL" if col['IS_NULLABLE'] == 'YES' else "NOT NULL"
            max_len = f"({col['CHARACTER_MAXIMUM_LENGTH']})" if col['CHARACTER_MAXIMUM_LENGTH'] else ""
            print(f"   • {col['COLUMN_NAME']}: {col['DATA_TYPE']}{max_len} {nullable}")
        
        # Kiểm tra dat_hang_csv
        print("\n" + "=" * 80)
        print("2. staging.dat_hang_csv")
        print("=" * 80)
        
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'staging'
        AND TABLE_NAME = 'dat_hang_csv'
        ORDER BY ORDINAL_POSITION
        """
        
        columns = db.execute_query(query)
        for col in columns:
            nullable = "NULL" if col['IS_NULLABLE'] == 'YES' else "NOT NULL"
            max_len = f"({col['CHARACTER_MAXIMUM_LENGTH']})" if col['CHARACTER_MAXIMUM_LENGTH'] else ""
            print(f"   • {col['COLUMN_NAME']}: {col['DATA_TYPE']}{max_len} {nullable}")
        
        db.close()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    check_table_structure()
