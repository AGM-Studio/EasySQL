import logging
import sys

logger = logging.getLogger('EasySQL')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)


def enable_debug():
    logger.setLevel(logging.DEBUG)
    handler.setLevel(logging.DEBUG)


def disable_debug():
    logger.setLevel(logging.INFO)
    handler.setLevel(logging.INFO)
