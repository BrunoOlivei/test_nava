import sys
from datetime import datetime

from loguru import logger


def setup_logger():
    logger.add(
        sys.stderr,
        format="{time} {level} {message}",
        filter="*",
        level="INFO",
    )



def get_logger():
    return logger
