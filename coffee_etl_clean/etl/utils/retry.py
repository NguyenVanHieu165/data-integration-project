from time import sleep
from ..logger import logger


def retry(times: int = 3, delay_sec: float = 1.0, label: str = "operation"):
    """
    Decorator dùng cho các hàm dễ lỗi (connect DB, connect RabbitMQ, ...)

    Ví dụ:

    @retry(times=5, delay_sec=2, label="connect_db")
    def connect_db():
        ...
    """

    def decorator(fn):
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, times + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    logger.warning(
                        "[%s] attempt %s/%s failed: %s",
                        label,
                        attempt,
                        times,
                        exc,
                    )
                    if attempt < times:
                        sleep(delay_sec)

            logger.error("[%s] all %s attempts failed", label, times)
            raise last_exc

        return wrapper

    return decorator
