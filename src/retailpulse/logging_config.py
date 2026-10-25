"""Centralized logging configuration for RetailPulse."""
import logging
from pathlib import Path


def configure_logging(log_dir="logs", level=logging.INFO):
    """Configure console and file logging."""
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("retailpulse")
    logger.setLevel(level)
    logger.propagate = False

    # Avoid duplicate handlers if this function is called again.
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(
        log_path / "pipeline.log", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
