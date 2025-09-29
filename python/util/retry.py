import asyncio
import functools
import logging

logger = logging.getLogger(__name__)


def async_retry_on_exception(
    exception: Exception, max_retries: int, wait_time_ms: int, factor: int
):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except exception as e:
                    retries += 1
                    if retries >= max_retries:
                        raise
                    logger.warning(
                        f"Retrying {func.__name__} after {e}, "
                        f"attempt {retries}/{max_retries}"
                    )
                    await asyncio.sleep(wait_time_ms * factor / 1000)

        return wrapper

    return decorator
