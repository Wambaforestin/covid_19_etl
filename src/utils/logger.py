import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logger(name, log_file='etl.log', level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Handler fichier
    log_path = Path('logs')
    log_path.mkdir(exist_ok=True)
    file_handler = RotatingFileHandler(log_path / log_file, maxBytes=10485760, backupCount=5)
    file_handler.setFormatter(formatter)
    
    # Handler console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger