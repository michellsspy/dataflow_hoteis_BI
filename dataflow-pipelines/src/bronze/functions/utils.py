# src/bronze/functions/utils.py

import logging
import sys

def setup_logging():
    """Configura o logger para ser usado em todo o projeto."""
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)