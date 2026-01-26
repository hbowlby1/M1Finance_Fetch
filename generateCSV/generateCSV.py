import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

CSV_DIR = os.path.join(os.getcwd(), "CSV")

class GenerateCSV:
    def __init__(self, df):
        self.df = df

    def save_to_csv(self, filename: str):
        if self.df is None or self.df.empty:
            logger.error("No data to save. DataFrame is None or empty.")
            return False
        
        # Ensure the CSV directory exists
        try:
            os.makedirs(CSV_DIR, exist_ok=True)
        except OSError:
            logger.exception("Error creating directory %s.", CSV_DIR)
            return False
        
        # Construct full path in CSV folder
        full_path = os.path.join(CSV_DIR, filename)
        
        try:
            self.df.to_csv(full_path, index=False)
            logger.info("CSV file saved to %s", full_path)
            return True
        except Exception:
            logger.exception("Error saving CSV file to %s.", full_path)
            return False
