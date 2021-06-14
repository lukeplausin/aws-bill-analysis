import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

log_handler = logging.StreamHandler(sys.stdout)
log_handler.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(levelname)s | %(asctime)s | %(pathname)s:%(lineno)d:%(name)s | %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)
# logger.addHandler(logging.NullHandler())
