import os
import sys
import yaml
import logging
from typing import Dict, Any

def load_yaml(filepath: str) -> Dict[str, Any]:
    """Load and parse a YAML file."""
    try:
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {filepath}")
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML file syntax at {filepath}: {e}")

def setup_logger(name: str = "ingestion-framework") -> logging.Logger:
    """Setup a standard logger that writes to stdout."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
