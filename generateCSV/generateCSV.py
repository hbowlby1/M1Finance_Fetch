import pandas as pd
import os

CSV_DIR = os.path.join(os.getcwd(), "CSV")

class GenerateCSV:
    def __init__(self, df):
        self.df = df

    def save_to_csv(self, filename: str):
        if self.df is None or self.df.empty:
            print("Error: No data to save. DataFrame is None or empty.")
            return False
        
        # Ensure the CSV directory exists
        try:
            os.makedirs(CSV_DIR, exist_ok=True)
        except OSError as e:
            print(f"Error creating directory {CSV_DIR}: {e}")
            return False
        
        # Construct full path in CSV folder
        full_path = os.path.join(CSV_DIR, filename)
        
        try:
            self.df.to_csv(full_path, index=False)
            print(f"CSV file saved to {full_path}")
            return True
        except Exception as e:
            print(f"Error saving CSV file to {full_path}: {e}")
            return False