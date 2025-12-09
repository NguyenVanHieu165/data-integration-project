# etl/broker/rabbitmq_client.py
import pika
import json
from typing import Dict, Callable, Optional
from ..logger import logger
from ..utils.retry import retry


class RabbitMQClient:
    """Client để kết nối và tương tác với RabbitMQ."""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        virtual_host: str = "/",
        heartbeat: int = 600,
        blocked_connection_timeout: int = 300
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.heartbeat = heartbeat
        self.blocked_connection_timeout = blocked_connection_timeout
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
    
    @retry(times=3, delay_sec=2, label="rabbitmq_connect")
    def connect(self):
        """Kết nối tới RabbitMQ."""
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=credentials,
            heartbeat=self.heartbeat,
            blocked_connection_timeout=self.blocked_connection_timeout
        )
        
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        logger.info("Kết nối RabbitMQ thành công: %s:%s", self.host, self.port)
    
    def declare_queue(self, queue_name: str, durable: bool = True):
        """Khai báo queue."""
        if not self.channel:
            raise RuntimeError("Chưa kết nối RabbitMQ")
        
        self.channel.queue_declare(queue=queue_name, durable=durable)
        logger.info("Đã khai báo queue: %s", queue_name)
    
    def publish(
        self,
        queue_name: str,
        message: Dict,
        persistent: bool = True
    ):
        """Gửi message vào queue."""
        if not self.channel:
            raise RuntimeError("Chưa kết nối RabbitMQ")
        
        # Import custom encoder
        from ..utils.json_encoder import json_dumps
        body = json_dumps(message)
        
        properties = pika.BasicProperties(
            delivery_mode=2 if persistent else 1,  # 2 = persistent
            content_type="application/json"
        )
        
        self.channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=body,
            properties=properties
        )
        
        # Log ở level DEBUG thay vì INFO để giảm noise
        # logger.debug("Đã gửi message vào queue %s", queue_name)
    
    def consume(
        self,
        queue_name: str,
        callback: Callable,
        auto_ack: bool = False
    ):
        """Nhận message từ queue."""
        if not self.channel:
            raise RuntimeError("Chưa kết nối RabbitMQ")
        
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=auto_ack
        )
        
        logger.info("Bắt đầu consume từ queue: %s", queue_name)
        self.channel.start_consuming()
    
    def ack_message(self, delivery_tag):
        """Xác nhận đã xử lý message."""
        if self.channel:
            self.channel.basic_ack(delivery_tag=delivery_tag)
    
    def nack_message(self, delivery_tag, requeue: bool = True):
        """Từ chối message."""
        if self.channel:
            self.channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
    
    def close(self):
        """Đóng kết nối."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Đã đóng kết nối RabbitMQ")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
