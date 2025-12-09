"""Script để tạo staging tables trong SQL Server."""
import os
from pathlib import Path
from dotenv import load_dotenv
from etl.db.sql_client import SQLServerClient
from etl.logger import logger

# Load environment variables
load_dotenv()

def run_sql_script(client: SQLServerClient, sql_file: str):
    """Đọc và thực thi SQL script."""
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Split by GO statements
    statements = [s.strip() for s in sql_content.split('GO') if s.strip()]
    
    for i, statement in enumerate(statements, 1):
        try:
            if statement:
                logger.info(f"Executing statement {i}/{len(statements)}...")
                client.execute_non_query(statement)
        except Exception as e:
            logger.error(f"Error executing statement {i}: {e}")
            # Continue with next statement
            continue

def main():
    """Main function."""
    # Database config
    db_config = {
        'server': os.getenv('TARGET_DB_HOST', 'localhost'),
        'database': os.getenv('TARGET_DB_NAME', 'newdata'),
        'trusted_connection': os.getenv('TARGET_DB_TRUSTED_CONNECTION', 'true').lower() == 'true'
    }
    
    sql_file = Path(__file__).parent / 'sql' / 'create_staging_tables.sql'
    
    logger.info("🚀 Bắt đầu tạo staging tables...")
    logger.info(f"Database: {db_config['server']}/{db_config['database']}")
    
    try:
        with SQLServerClient(**db_config) as client:
            run_sql_script(client, str(sql_file))
        
        logger.info("✅ Tạo staging tables thành công!")
    
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}")
        raise

if __name__ == '__main__':
    main()
