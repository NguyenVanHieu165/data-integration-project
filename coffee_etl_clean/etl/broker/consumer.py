# etl/broker/consumer.py
import json
from typing import Callable, Optional
from .rabbitmq_client import RabbitMQClient
from ..logger import logger


class StagingConsumer:
    """Consumer nhận message từ RabbitMQ và đổ vào staging."""
    
    def __init__(
        self,
        rabbitmq_client: RabbitMQClient,
        staging_writer: Callable
    ):
        """
        Args:
            rabbitmq_client: RabbitMQ client
            staging_writer: Function để ghi dữ liệu vào staging
                           Signature: staging_writer(data: dict) -> bool
        """
        self.client = rabbitmq_client
        self.staging_writer = staging_writer
        self.processed_count = 0
        self.error_count = 0
    
    def start_consuming(
        self,
        queue_name: str,
        max_messages: Optional[int] = None
    ):
        """
        Bắt đầu consume message từ queue.
        
        Args:
            queue_name: Tên queue
            max_messages: Số message tối đa (None = không giới hạn)
        """
        logger.info("[Consumer] Bắt đầu consume từ queue: %s", queue_name)
        
        def callback(ch, method, properties, body):
            try:
                # Parse message
                message = json.loads(body.decode("utf-8"))
                data = message.get("data", {})
                
                # Ghi vào staging
                success = self.staging_writer(data)
                
                if success:
                    self.processed_count += 1
                    self.client.ack_message(method.delivery_tag)
                    
                    if self.processed_count % 100 == 0:
                        logger.info(
                            "[Consumer] Đã xử lý %s messages",
                            self.processed_count
                        )
                else:
                    self.error_count += 1
                    self.client.nack_message(method.delivery_tag, requeue=False)
                    logger.warning(
                        "[Consumer] Lỗi ghi staging, message bị reject"
                    )
                
                # Dừng nếu đạt max_messages
                if max_messages and self.processed_count >= max_messages:
                    ch.stop_consuming()
            
            except json.JSONDecodeError as e:
                self.error_count += 1
                logger.error("[Consumer] Lỗi parse JSON: %s", e)
                self.client.nack_message(method.delivery_tag, requeue=False)
            
            except Exception as e:
                self.error_count += 1
                logger.error("[Consumer] Lỗi xử lý message: %s", e)
                self.client.nack_message(method.delivery_tag, requeue=True)
        
        try:
            self.client.consume(queue_name, callback, auto_ack=False)
        except KeyboardInterrupt:
            logger.info("[Consumer] Dừng consume bởi user")
        finally:
            logger.info(
                "[Consumer] Tổng kết: %s thành công, %s lỗi",
                self.processed_count,
                self.error_count
            )
