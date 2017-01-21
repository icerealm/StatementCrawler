import os
from logging.config import fileConfig

DEFAULT_LOG_PATH = "../../conf/logging_config.ini"

module_dir = os.path.dirname(__file__)
log_full_path = os.path.join(module_dir, DEFAULT_LOG_PATH)

logger = None

def init_logger():
    global logger
    if not logger:
        fileConfig(log_full_path)

