from configparser import SafeConfigParser
import os

DEFAULT_CFG_PATH = "../../conf/statement_reader.conf"
CONFIG = "APP_CONFIGURATION"

module_dir = os.path.dirname(__file__)
cfg_full_path = os.path.join(module_dir, DEFAULT_CFG_PATH)
app_configuration = None

def init_configuration():
    global app_configuration    
    if not app_configuration:
        app_configuration = SafeConfigParser()
        app_configuration.read(cfg_full_path, "utf8")

def get_configuration(): 
    return app_configuration