import logging
import os
from datetime import datetime


LOG_FILE_PATH = "logger.log"

logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="[ %(asctime)s ] | %(levelname)s | %(message)s| %(filename)s:%(lineno)d",
    level=logging.DEBUG,
)


if __name__ == "__main__":
    logging.debug('This is a debug message')
    logging.info('This is an info message')
    logging.warning('This is a warning message')
    logging.error('This is an error message')
