import logging
from logging.handlers import RotatingFileHandler

def setup_logging(log_file="app.log", level=logging.INFO, enabled=True):
    if not enabled:
        logging.disable(logging.CRITICAL)  # disables all logging
        return

    logging.disable(logging.NOTSET)  # ensure logging is enabled if previously disabled
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=[
            RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=3),
            logging.StreamHandler()
        ],
    )
