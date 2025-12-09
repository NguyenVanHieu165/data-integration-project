# etl/broker/producer.py
from typing import Dict, List
from pathlib import Path
from .rabbitmq_client import RabbitMQClient
from ..readers.csv_staging_reader import csv_staging_reader
from ..logger import logger


class CSVProducer:
    """Producer đọc file CSV và gửi vào RabbitMQ."""
    
    def __init__(self, rabbitmq_client: RabbitMQClient):
        self.client = rabbitmq_client
    
    def produce_from_csv(
        self,
        file_path: str,
        queue_name: str,
        batch_size: int = 100
    ):
        """
        Đọc file CSV và gửi từng record vào queue.
        
        Args:
            file_path: Đường dẫn file CSV
            queue_name: Tên queue RabbitMQ
            batch_size: Số record gửi mỗi lần log
        """
        logger.info(
            "[Producer] Bắt đầu đọc file: %s → queue: %s",
            file_path,
            queue_name
        )
        
        # Khai báo queue
        self.client.declare_queue(queue_name)
        
        count = 0
        errors = 0
        
        try:
            for row in csv_staging_reader(file_path):
                try:
                    # Thêm metadata
                    message = {
                        "data": row,
                        "source_file": Path(file_path).name,
                        "queue": queue_name
                    }
                    
                    self.client.publish(queue_name, message)
                    count += 1
                    
                    if count % batch_size == 0:
                        logger.info(
                            "[Producer] Đã gửi %s records vào queue %s",
                            count,
                            queue_name
                        )
                
                except Exception as e:
                    errors += 1
                    logger.error(
                        "[Producer] Lỗi gửi record: %s",
                        e,
                        extra={"extra_data": {"row": row, "error": str(e)}}
                    )
        
        except Exception as e:
            logger.error("[Producer] Lỗi đọc file %s: %s", file_path, e)
            raise
        
        logger.info(
            "[Producer] Hoàn thành: %s records thành công, %s lỗi",
            count,
            errors
        )
        
        return count, errors


class MultiFileProducer:
    """Producer xử lý nhiều file CSV cùng lúc."""
    
    def __init__(self, rabbitmq_client: RabbitMQClient):
        self.client = rabbitmq_client
        self.producer = CSVProducer(rabbitmq_client)
    
    def produce_multiple(
        self,
        file_queue_mapping: Dict[str, str]
    ):
        """
        Đọc nhiều file và gửi vào các queue tương ứng.
        
        Args:
            file_queue_mapping: Dict mapping file_path -> queue_name
            Ví dụ: {
                "data/nguyenlieu.csv": "queue_nguyenlieu",
                "data/tensanpham.csv": "queue_tensanpham"
            }
        """
        results = {}
        
        for file_path, queue_name in file_queue_mapping.items():
            try:
                count, errors = self.producer.produce_from_csv(
                    file_path,
                    queue_name
                )
                results[file_path] = {
                    "success": count,
                    "errors": errors,
                    "queue": queue_name
                }
            except Exception as e:
                logger.error(
                    "[MultiFileProducer] Lỗi xử lý file %s: %s",
                    file_path,
                    e
                )
                results[file_path] = {
                    "success": 0,
                    "errors": -1,
                    "error": str(e)
                }
        
        return results
