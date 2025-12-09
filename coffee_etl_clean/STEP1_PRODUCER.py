"""
STEP 1: PRODUCER
================
Đọc dữ liệu từ CSV + SQL Server → Gửi vào RabbitMQ queues

Luồng:
- CSV Files (data/) → Producer → RabbitMQ
- SQL Server (ComVanPhong) → Producer → RabbitMQ

Output: Messages trong RabbitMQ queues
"""

import time
from pathlib import Path
from datetime import datetime
from typing import Dict

from etl.broker.rabbitmq_client import RabbitMQClient
from etl.db.database_factory import DatabaseFactory, SourceDBReader
from etl.readers.csv_staging_reader import csv_staging_reader
from etl.config import settings
from etl.logger import logger


class ProducerPipeline:
    """Pipeline Producer - Gửi dữ liệu vào RabbitMQ."""
    
    def __init__(self):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.stats = {
            "csv": {},
            "sql": {}
        }
    
    def run(self):
        logger.info("=" * 80)
        logger.info("STEP 1: PRODUCER PIPELINE")
        logger.info("Run ID: %s", self.run_id)
        logger.info("=" * 80)
        
        try:
            with RabbitMQClient(
                host=settings.RABBITMQ_HOST,
                port=settings.RABBITMQ_PORT,
                username=settings.RABBITMQ_USER,
                password=settings.RABBITMQ_PASSWORD,
            ) as rabbitmq:
                
                logger.info("\n📄 Producer 1: CSV Files → RabbitMQ")
                logger.info("-" * 80)
                csv_stats = self.produce_from_csv(rabbitmq)
                self.stats["csv"] = csv_stats
                
                logger.info("\n💾 Producer 2: SQL Server → RabbitMQ")
                logger.info("-" * 80)
                sql_stats = self.produce_from_sql(rabbitmq)
                self.stats["sql"] = sql_stats
                
                self.print_summary()
                
        except Exception as e:
            logger.error("❌ Lỗi Producer pipeline: %s", e, exc_info=True)
            raise
    
    def produce_from_csv(self, rabbitmq: RabbitMQClient) -> Dict[str, int]:
        """Producer từ CSV files."""
        stats = {}
        
        script_dir = Path(__file__).parent
        data_dir = script_dir / "data"
        
        # Mapping: CSV file → Queue name
        csv_files = {
            "khachhang.csv": "queue_khach_hang",
            "loaisanpham.csv": "queue_loai_mon",
            "tensanpham.csv": "queue_mon",
            "nguyenlieu.csv": "queue_nguyen_lieu",
            "dathang.csv": "queue_dat_hang",
        }
        
        for file_name, queue_name in csv_files.items():
            file_path = data_dir / file_name
            
            if not file_path.exists():
                logger.warning("   ⚠️  Không tìm thấy: %s", file_name)
                continue
            
            # Declare queue
            rabbitmq.declare_queue(queue_name, durable=True)
            count = 0
            
            try:
                for row in csv_staging_reader(str(file_path)):
                    message = {
                        "source": "csv",
                        "entity_type": queue_name.replace("queue_", ""),
                        "data": row,
                        "metadata": {
                            "file": file_name,
                            "extract_time": datetime.now().isoformat(),
                            "run_id": self.run_id
                        },
                    }
                    rabbitmq.publish(queue_name, message, persistent=True)
                    count += 1
                
                stats[queue_name] = count
                logger.info("   ✓ %s: %s messages → %s", file_name, count, queue_name)
                
            except Exception as e:
                logger.error("   ✗ Lỗi %s: %s", file_name, e)
        
        return stats
    
    def produce_from_sql(self, rabbitmq: RabbitMQClient) -> Dict[str, int]:
        """Producer từ SQL Server."""
        stats = {}
        source_db = DatabaseFactory.create_source_db()
        
        try:
            source_db.connect()
            reader = SourceDBReader(source_db)
            
            # Auto-discovery tables
            tables = reader.get_all_tables(schema="dbo")
            
            for table in tables:
                if table.lower() in ["sysdiagrams"]:
                    continue
                
                # Infer entity type từ table name
                entity_type = self.infer_entity_type(table)
                queue_name = f"queue_{entity_type}"
                
                # Declare queue
                rabbitmq.declare_queue(queue_name, durable=True)
                
                try:
                    data = reader.read_table(table, schema="dbo")
                    
                    from etl.utils.json_encoder import convert_sql_row_to_json_compatible
                    
                    for row in data:
                        json_row = convert_sql_row_to_json_compatible(row)
                        message = {
                            "source": "sql",
                            "entity_type": entity_type,
                            "data": json_row,
                            "metadata": {
                                "table": table,
                                "database": settings.SOURCE_DB_NAME,
                                "extract_time": datetime.now().isoformat(),
                                "run_id": self.run_id
                            },
                        }
                        rabbitmq.publish(queue_name, message, persistent=True)
                    
                    stats[queue_name] = len(data)
                    logger.info("   ✓ %s: %s messages → %s", table, len(data), queue_name)
                    
                except Exception as e:
                    logger.error("   ✗ Lỗi %s: %s", table, e)
        
        finally:
            source_db.close()
        
        return stats
    
    def infer_entity_type(self, name: str) -> str:
        """Infer entity type từ table name."""
        mapping = {
            "khachhang": "khach_hang",
            "khach_hang": "khach_hang",
            "khach_hang_tbl": "khach_hang",
            "mon": "mon",
            "mon_tbl": "mon",
            "nguyenlieu": "nguyen_lieu",
            "nguyen_lieu": "nguyen_lieu",
            "nguyen_lieu_tbl": "nguyen_lieu",
            "loaimon": "loai_mon",
            "loai_mon": "loai_mon",
            "loai_mon_tbl": "loai_mon",
            "dathang": "dat_hang",
            "dat_hang": "dat_hang",
            "dat_hang_tbl": "dat_hang",
        }
        normalized = name.lower().replace("-", "_").replace(" ", "_")
        return mapping.get(normalized, normalized)
    
    def print_summary(self):
        logger.info("\n" + "=" * 80)
        logger.info("📊 PRODUCER SUMMARY")
        logger.info("=" * 80)
        
        logger.info("\n📄 CSV → RabbitMQ:")
        csv_total = 0
        for queue, count in self.stats["csv"].items():
            logger.info("   • %s: %s messages", queue, count)
            csv_total += count
        logger.info("   TỔNG CSV: %s messages", csv_total)
        
        logger.info("\n💾 SQL → RabbitMQ:")
        sql_total = 0
        for queue, count in self.stats["sql"].items():
            logger.info("   • %s: %s messages", queue, count)
            sql_total += count
        logger.info("   TỔNG SQL: %s messages", sql_total)
        
        logger.info("\n✅ TỔNG: %s messages đã gửi vào RabbitMQ", csv_total + sql_total)
        logger.info("=" * 80)


def main():
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 28 + "STEP 1: PRODUCER" + " " * 35 + "║")
    print("║" + " " * 20 + "CSV + SQL Server → RabbitMQ" + " " * 32 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    pipeline = ProducerPipeline()
    
    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Pipeline bị dừng bởi user (Ctrl+C)")
    except Exception as e:
        logger.error("❌ Pipeline thất bại: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()
