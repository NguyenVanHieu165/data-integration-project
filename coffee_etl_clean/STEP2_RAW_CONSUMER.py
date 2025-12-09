"""
STEP 2: RAW CONSUMER
====================
Nhận messages từ RabbitMQ → Ghi vào staging/raw/*.csv (RAW ZONE)

Luồng:
- RabbitMQ queues → Consumer → staging/raw/*.csv

Output: CSV files trong staging/raw/
- staging/raw/khach_hang_csv_YYYYMMDD_HHMMSS.csv
- staging/raw/khach_hang_sql_YYYYMMDD_HHMMSS.csv
- ...
"""

import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from etl.broker.rabbitmq_client import RabbitMQClient
from etl.config import settings
from etl.logger import logger


class RawConsumerPipeline:
    """Pipeline Raw Consumer - Ghi raw data vào CSV files."""
    
    def __init__(self):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Tạo thư mục staging/raw
        self.raw_dir = Path("staging") / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        
        self.stats = {}
        self.file_writers = {}  # Cache CSV writers
    
    def run(self):
        logger.info("=" * 80)
        logger.info("STEP 2: RAW CONSUMER PIPELINE")
        logger.info("Run ID: %s", self.run_id)
        logger.info("Output: staging/raw/")
        logger.info("=" * 80)
        
        try:
            # Danh sách queues cần consume
            queues = [
                ("queue_khach_hang", "khach_hang"),
                ("queue_loai_mon", "loai_mon"),
                ("queue_mon", "mon"),
                ("queue_nguyen_lieu", "nguyen_lieu"),
                ("queue_dat_hang", "dat_hang"),
            ]
            
            for queue_name, entity_type in queues:
                logger.info("\n📥 Processing: %s", queue_name)
                self.consume_queue(queue_name, entity_type)
            
            # Đóng tất cả file writers
            self.close_all_writers()
            
            self.print_summary()
            
        except Exception as e:
            logger.error("❌ Lỗi Raw Consumer pipeline: %s", e, exc_info=True)
            raise
    
    def consume_queue(self, queue_name: str, entity_type: str):
        """Consume một queue và ghi vào CSV files."""
        with RabbitMQClient(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            username=settings.RABBITMQ_USER,
            password=settings.RABBITMQ_PASSWORD,
        ) as rabbitmq:
            
            try:
                # Kiểm tra số message trong queue
                method_frame = rabbitmq.channel.queue_declare(
                    queue=queue_name, durable=True, passive=True
                )
                message_count = method_frame.method.message_count
            except Exception:
                logger.warning("   Queue không tồn tại: %s", queue_name)
                return
            
            if message_count == 0:
                logger.info("   Queue rỗng")
                return
            
            logger.info("   Messages: %s", message_count)
            
            consumed = 0
            csv_count = 0
            sql_count = 0
            
            def callback(ch, method, properties, body):
                nonlocal consumed, csv_count, sql_count
                
                try:
                    message = json.loads(body.decode("utf-8"))
                    source = message.get("source", "unknown")
                    data = message.get("data", {})
                    metadata = message.get("metadata", {})
                    
                    # Ghi vào CSV file tương ứng
                    self.write_to_csv(entity_type, source, data, metadata)
                    
                    if source == "csv":
                        csv_count += 1
                    elif source == "sql":
                        sql_count += 1
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    consumed += 1
                    
                except Exception as e:
                    logger.error("   Lỗi xử lý message: %s", e)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            rabbitmq.channel.basic_qos(prefetch_count=1)
            rabbitmq.channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )
            
            # Consume messages
            while consumed < message_count:
                rabbitmq.connection.process_data_events(time_limit=1)
            
            self.stats[entity_type] = {
                "total": consumed,
                "csv": csv_count,
                "sql": sql_count
            }
            
            logger.info("   ✓ Consumed: %s (CSV: %s, SQL: %s)", consumed, csv_count, sql_count)
    
    def write_to_csv(self, entity_type: str, source: str, data: Dict, metadata: Dict):
        """Ghi một row vào CSV file."""
        # Tạo file name: entity_source_runid.csv
        file_key = f"{entity_type}_{source}"
        
        if file_key not in self.file_writers:
            file_name = f"{entity_type}_{source}_{self.run_id}.csv"
            file_path = self.raw_dir / file_name
            
            # Mở file và tạo CSV writer
            f = open(file_path, "w", encoding="utf-8-sig", newline="")
            
            # Lấy tất cả columns từ data + metadata
            all_keys = list(data.keys()) + ["_source", "_extract_time", "_run_id"]
            
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction='ignore')
            writer.writeheader()
            
            self.file_writers[file_key] = {
                "file": f,
                "writer": writer,
                "path": file_path,
                "count": 0
            }
        
        # Ghi row
        writer_info = self.file_writers[file_key]
        writer = writer_info["writer"]
        
        # Merge data + metadata
        row = {**data}
        row["_source"] = source
        row["_extract_time"] = metadata.get("extract_time", "")
        row["_run_id"] = metadata.get("run_id", "")
        
        # Chỉ ghi các fields có trong fieldnames, bỏ qua fields thừa
        writer.writerow(row)
        writer_info["count"] += 1
    
    def close_all_writers(self):
        """Đóng tất cả file writers."""
        for file_key, writer_info in self.file_writers.items():
            writer_info["file"].close()
            logger.info("   ✓ Đã ghi %s rows vào %s", 
                       writer_info["count"], 
                       writer_info["path"].name)
    
    def print_summary(self):
        logger.info("\n" + "=" * 80)
        logger.info("📊 RAW CONSUMER SUMMARY")
        logger.info("=" * 80)
        
        logger.info("\n📁 Output Directory: %s", self.raw_dir)
        
        logger.info("\n📄 Files Created:")
        for file_key, writer_info in sorted(self.file_writers.items()):
            logger.info("   • %s (%s rows)", 
                       writer_info["path"].name, 
                       writer_info["count"])
        
        logger.info("\n📊 Statistics by Entity:")
        total_all = 0
        for entity, stats in sorted(self.stats.items()):
            logger.info("   • %s: %s (CSV: %s, SQL: %s)", 
                       entity, 
                       stats["total"], 
                       stats["csv"], 
                       stats["sql"])
            total_all += stats["total"]
        
        logger.info("\n✅ TỔNG: %s rows đã ghi vào RAW ZONE", total_all)
        logger.info("=" * 80)


def main():
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 26 + "STEP 2: RAW CONSUMER" + " " * 33 + "║")
    print("║" + " " * 22 + "RabbitMQ → staging/raw/*.csv" + " " * 29 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    pipeline = RawConsumerPipeline()
    
    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Pipeline bị dừng bởi user (Ctrl+C)")
    except Exception as e:
        logger.error("❌ Pipeline thất bại: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()
