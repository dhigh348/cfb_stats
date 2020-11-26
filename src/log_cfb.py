import sys
import os
import logging
from pathlib import Path


# setting the logging configuration
logging.basicConfig(
    format='%(asctime)s - %(levelname)s: %(message)s',
    level=logging.INFO,
    stream=sys.stdout)
log = logging.getLogger()


class CFBDataLogger:

    def __init__(self):
        self.log = log

    def info(self, msg: str):
        """
        Logging the message for the logger.
        :param msg: message to deliver to the logger
        """
        self.log.info(msg)
