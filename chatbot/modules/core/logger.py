"""
Loglama yapılandırması
======================
Tüm loglar hem console'a hem dosyaya yazılır.
"""

import logging
import os
from datetime import datetime

LOG_DIR = os.getenv("LOG_DIR", "/app/logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, f"chatbot_{datetime.now().strftime('%Y%m%d')}.log")

# Formatter
fmt = logging.Formatter(
    "%(asctime)s │ %(levelname)-5s │ %(name)-10s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# File handler
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(fmt)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(fmt)

# Root logger
root = logging.getLogger()
root.setLevel(logging.DEBUG)
root.addHandler(file_handler)
root.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
