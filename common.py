import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
from threading import Lock

# Initialize global variables
config = None
write_lock = Lock()
LOG_FILE_PATH = None
TELEMETRY_FILE_PATH = None
ARCHIVE_FILE_PATH = None
BUFFER_A = None
BUFFER_B = None
ACTIVE_BUFFER_FILE = None

# Logging configuration
LOGGING_MODES = {
    "low": {
        "console_level": logging.WARNING,
        "file_level": logging.ERROR,
        "format": '%(asctime)s - %(levelname)s - %(message)s'
    },
    "standard": {
        "console_level": logging.INFO,
        "file_level": logging.INFO,
        "format": '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    },
    "verbose": {
        "console_level": logging.DEBUG,
        "file_level": logging.DEBUG,
        "format": '%(asctime)s - %(name)s - %(levelname)s - %(message)s - [%(threadName)s] %(funcName)s:%(lineno)d'
    }
}

def initialize():
    global config, LOG_FILE_PATH, TELEMETRY_FILE_PATH, ARCHIVE_FILE_PATH, BUFFER_A, BUFFER_B, ACTIVE_BUFFER_FILE
    
    # Load configuration
    with open("config.json", "r") as config_file:
        config = json.load(config_file)
    
    # Initialize file paths
    LOG_FILE_PATH = Path(config["logFilePath"])
    TELEMETRY_FILE_PATH = Path(config["telemetryFilePath"])
    ARCHIVE_FILE_PATH = Path(config["archiveFilePath"])
    BUFFER_A = TELEMETRY_FILE_PATH.parent / "buffer_a.json"
    BUFFER_B = TELEMETRY_FILE_PATH.parent / "buffer_b.json"
    ACTIVE_BUFFER_FILE = TELEMETRY_FILE_PATH.parent / "active_buffer.txt"

    # Ensure directories exist
    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    TELEMETRY_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    ARCHIVE_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_config():
    global config
    if config is None:
        initialize()
    return config

def setup_logging(module_name, log_file_path=None):
    if config is None:
        initialize()
    
    logging_mode = config.get("loggingMode", "standard").lower()
    if logging_mode not in LOGGING_MODES:
        logging_mode = "standard"
    
    log_config = LOGGING_MODES[logging_mode]
    logger = logging.getLogger(module_name)
    
    # Ensure log directory exists
    if log_file_path is None:
        log_file_path = LOG_FILE_PATH
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Setup handlers
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(log_config["file_level"])
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_config["console_level"])
    
    # Setup formatter
    formatter = logging.Formatter(log_config["format"])
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Configure logger
    logger.setLevel(min(log_config["console_level"], log_config["file_level"]))
    logger.handlers = []
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def clear_old_logs():
    if LOG_FILE_PATH.exists():
        cutoff_time = datetime.now() - timedelta(days=config["logRetentionDays"])
        with open(LOG_FILE_PATH, 'r') as f:
            lines = f.readlines()
        with open(LOG_FILE_PATH, 'w') as f:
            for line in lines:
                log_time = datetime.strptime(line.split(' - ')[0], '%Y-%m-%d %H:%M:%S,%f')
                if log_time > cutoff_time:
                    f.write(line)

def get_active_buffer():
    if not ACTIVE_BUFFER_FILE.exists():
        with open(ACTIVE_BUFFER_FILE, 'w') as f:
            f.write('A')
        return BUFFER_A
    with open(ACTIVE_BUFFER_FILE, 'r') as f:
        active = f.read().strip()
    return BUFFER_A if active == 'A' else BUFFER_B

def switch_buffer():
    with open(ACTIVE_BUFFER_FILE, 'r') as f:
        current = f.read().strip()
    with open(ACTIVE_BUFFER_FILE, 'w') as f:
        f.write('B' if current == 'A' else 'A')
    return BUFFER_B if current == 'A' else BUFFER_A

# Define what can be imported from this module
__all__ = [
    'setup_logging',
    'get_config',
    'get_active_buffer',
    'switch_buffer',
    'write_lock',
    'ARCHIVE_FILE_PATH',
    'TELEMETRY_FILE_PATH',
    'LOG_FILE_PATH',
    'BUFFER_A',
    'BUFFER_B',
    'ACTIVE_BUFFER_FILE'
]

# Initialize module when imported
initialize()
