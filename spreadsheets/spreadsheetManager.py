"""
Creates or manages the spreadsheets containing the user's M1 Finance
data using the Google Sheets API mixed with gsheets
"""

import os
import gspread
import json
import pandas as pd
import numpy as np
import requests
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound, APIError
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound, APIError


class spreadsheetManager:
    def __init__(
        self,
        spreadsheetName=None,
        credentialsPath="credentials.json",
        CSVFolderPath="CSV",
        createNewSpreadSheet=False,
        generateTaxLotsSheets=False,
    ):
        try:
            self.spreadsheetName = spreadsheetName
            self.credentialsPath = credentialsPath
            self.CSVFolderPath = CSVFolderPath
            self.createNewSpreadSheet = createNewSpreadSheet
            self.generateTaxLotsSheets = generateTaxLotsSheets
            self.SpreadSheetID = None
            self.gc = None

            # Validate credentials file exists
            if not os.path.exists(self.credentialsPath):
                raise FileNotFoundError(
                    f"Credentials file not found at {self.credentialsPath}"
                )

        except FileNotFoundError as e:
            print(f"Initialization error: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error during initialization: {e}")
            raise

    def authenticate_google_sheets(self):
        """
        Authenticates and returns a gspread client

        :return: gspread Client object or None if failed
        """
        try:
            # Validate credentials file exists
            if not os.path.exists(self.credentialsPath):
                print(f"Credentials file not found at {self.credentialsPath}")
                return None

            # Initialize gspread client
            self.gc = gspread.service_account(filename=self.credentialsPath)
            return self.gc

        except Exception as e:
            print(f"Google Sheets authentication error: {e}")
            return None

    def fetch_spreadsheet(self):
        """
        Docstring for fetch_spreadsheet

        :param self: Description
        """
        try:
            sh = self.gc.open(self.spreadsheetName)
            self.SpreadSheetID = sh.id
            return sh
        except SpreadsheetNotFound:
            print(f"Spreadsheet named '{self.spreadsheetName}' not found.")
            return None
        except Exception as e:
            print(f"Error fetching spreadsheet: {e}")
            return None

    # def create_spreadsheet(self, title="M1 Finance Data"):
    #     '''
    #     Creates a new Google Spreadsheet or returns existing spreadsheet ID

    #     :param title: Title for the new spreadsheet
    #     :return: Spreadsheet ID or None if failed
    #     NOT IN USE UNTIL I FIGURE OUT HOW TO RESOLVE 403 ERRORS WHEN
    #     USING SERVICE ACCOUNT TO CREATE SPREADSHEETS.
    #     '''
    #     try:
    #         # Validate credentials
    #         if not os.path.exists(self.credentialsPath):
    #             print(f"Credentials file not found at {self.credentialsPath}")
    #             return None

    #         if self.createNewSpreadSheet or not self.spreadSheetID:
    #             # Create new spreadsheet
    #             sh = self.gc.create(title)
    #             self.spreadSheetID = sh.id
    #             print(f"Spreadsheet created with ID: {self.spreadSheetID}")
    #             return self.spreadSheetID
    #         else:
    #             # Validate existing spreadsheet ID
    #             try:
    #                 sh = self.gc.open_by_key(self.spreadSheetID)
    #                 print(f"Using existing Spreadsheet ID: {self.spreadSheetID}")
    #                 return self.spreadSheetID
    #             except SpreadsheetNotFound:
    #                 print(f"Spreadsheet with ID {self.spreadSheetID} not found. Creating new one.")
    #                 sh = self.gc.create(title)
    #                 self.spreadSheetID = sh.id
    #                 print(f"New spreadsheet created with ID: {self.spreadSheetID}")
    #                 return self.spreadSheetID

    #     except APIError as e:
    #         print(f"Google Sheets API error: {e}")
    #         return None
    #     except PermissionError as e:
    #         print(f"Permission error accessing credentials: {e}")
    #         return None
    #     except Exception as e:
    #         print(f"Unexpected error creating spreadsheet: {e}")
    #         return None
    def create_holdings_sheet(self):
        """
        Creates a holdings worksheet and uploads holdings data from CSV

        :return: True if successful, False otherwise
        """
        try:
            if not self.SpreadSheetID:
                print("Spreadsheet ID is not set. Please create a spreadsheet first.")
                return False

            # Validate CSV folder exists
            if not os.path.exists(self.CSVFolderPath):
                print(f"CSV folder not found at {self.CSVFolderPath}")
                return False

            # Open spreadsheet
            sh = self.gc.open_by_key(self.SpreadSheetID)

            # Check if holdings sheet already exists
            try:
                worksheet = sh.worksheet("Holdings")
                print("Holdings sheet already exists. Updating data...")
            except WorksheetNotFound:
                worksheet = sh.add_worksheet(title="Holdings", rows="100", cols="20")
                print("Created new Holdings worksheet.")

            # Validate and read CSV file
            holdings_csv_path = os.path.join(self.CSVFolderPath, "holdings.csv")
            if not os.path.exists(holdings_csv_path):
                print(f"Holdings CSV file not found at {holdings_csv_path}")
                return False

            try:
                df = pd.read_csv(holdings_csv_path)
                if df.empty:
                    print("Holdings CSV file is empty.")
                    return False
                # set columns to respective types
                df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
                # currency columns
                df["average_share_price"] = pd.to_numeric(
                    df["average_share_price"], errors="coerce"
                )
                df["total_cost"] = pd.to_numeric(df["total_cost"], errors="coerce")
                df["current_value"] = pd.to_numeric(
                    df["current_value"], errors="coerce"
                )
                df["unrealized_gain"] = pd.to_numeric(
                    df["unrealized_gain"], errors="coerce"
                )
                # percentage columns
                df["unrealized_gain_percent"] = (
                    pd.to_numeric(df["unrealized_gain_percent"], errors="coerce") / 100
                )
                df["maintenance_margin_percent"] = (
                    pd.to_numeric(df["maintenance_margin_percent"], errors="coerce")
                    / 100
                )
            except pd.errors.EmptyDataError:
                print("Holdings CSV file is empty or invalid.")
                return False
            except pd.errors.ParserError as e:
                print(f"Error parsing holdings CSV: {e}")
                return False

            # Upload data to sheet
            worksheet.clear()  # Clear existing data
            worksheet.update([df.columns.values.tolist()] + df.values.tolist())

            # format currency columns to USD
            currency_format = {
                "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}
            }
            percentage_format = {
                "numberFormat": {"type": "PERCENT", "pattern": "0.00%"}
            }
            currency_columns = [
                "average_share_price",
                "total_cost",
                "current_value",
                "unrealized_gain",
            ]
            percentage_columns = [
                "unrealized_gain_percent",
                "maintenance_margin_percent",
            ]
            for col in currency_columns:
                if col in df.columns:
                    col_index = df.columns.get_loc(col) + 1  # gspread is 1-indexed
                    worksheet.format(
                        f"{chr(64 + col_index)}2:{chr(64 + col_index)}{len(df)+1}",
                        currency_format,
                    )
            for col in percentage_columns:
                if col in df.columns:
                    col_index = df.columns.get_loc(col) + 1  # gspread is 1-indexed
                    worksheet.format(
                        f"{chr(64 + col_index)}2:{chr(64 + col_index)}{len(df)+1}",
                        percentage_format,
                    )
            print("Holdings sheet updated with data successfully.")
            return True

        except APIError as e:
            print(f"Google Sheets API error: {e}")
            return False
        except SpreadsheetNotFound:
            print(f"Spreadsheet with ID {self.spreadSheetID} not found.")
            return False
        except PermissionError as e:
            print(f"Permission error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error creating holdings sheet: {e}")
            return False

    def create_tax_lots_sheet(self, lot_type="open"):
        """
        Creates a tax lots worksheet and uploads tax lots data from CSV

        :param lot_type: Type of tax lots ("open" or "closed")
        :return: True if successful, False otherwise
        """
        try:
            if not self.SpreadSheetID:
                print("Spreadsheet ID is not set. Please create a spreadsheet first.")
                return False

            # Validate lot_type parameter
            if lot_type not in ["open", "closed"]:
                print(f"Invalid lot_type: {lot_type}. Must be 'open' or 'closed'.")
                return False

            # Validate CSV folder exists
            if not os.path.exists(self.CSVFolderPath):
                print(f"CSV folder not found at {self.CSVFolderPath}")
                return False

            # Open spreadsheet
            sh = self.gc.open_by_key(self.SpreadSheetID)

            # Determine sheet title
            sheet_title = "Open Tax Lots" if lot_type == "open" else "Closed Tax Lots"

            # Check if tax lots sheet already exists
            try:
                worksheet = sh.worksheet(sheet_title)
                print(f"{sheet_title} sheet already exists. Updating data...")
            except WorksheetNotFound:
                worksheet = sh.add_worksheet(title=sheet_title, rows="100", cols="20")
                print(f"Created new {sheet_title} worksheet.")

            # Validate and read CSV file
            tax_lots_csv_path = os.path.join(
                self.CSVFolderPath, f"{lot_type}_tax_lots.csv"
            )
            if not os.path.exists(tax_lots_csv_path):
                print(
                    f"{lot_type.capitalize()} tax lots CSV file not found at {tax_lots_csv_path}"
                )
                return False

            try:
                df = pd.read_csv(tax_lots_csv_path)
                if df.empty:
                    print(f"{lot_type.capitalize()} tax lots CSV file is empty.")
                    return False
                # column cleanup and type setting
                df["symbol"] = df["symbol"].astype(str)
                df["cusip"] = df["cusip"].astype(str)
                df["acquisitionDate"] = df["acquisitionDate"].astype(str)
                df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
                df["costBasis"] = pd.to_numeric(df["costBasis"], errors="coerce")
                df["shortLongTermHolding"] = df["shortLongTermHolding"].astype(str)
                df["unrealizedGainLoss"] = pd.to_numeric(
                    df["unrealizedGainLoss"], errors="coerce"
                )
                df["closeDate"] = df["closeDate"].astype(str)
                df["shortTermRealizedGainLoss"] = pd.to_numeric(
                    df["shortTermRealizedGainLoss"], errors="coerce"
                )
                df["longTermRealizedGainLoss"] = pd.to_numeric(
                    df["longTermRealizedGainLoss"], errors="coerce"
                )
                df["washSaleIndicator"] = df["washSaleIndicator"].astype("boolean")

                # columns to remove
                columns_to_remove = ["id", "__typename"]
                df.drop(
                    columns=[col for col in columns_to_remove if col in df.columns],
                    inplace=True,
                )

                # fill NaN values with null
                df.replace({pd.NA: None, np.nan: None}, inplace=True)

            except pd.errors.EmptyDataError:
                print(f"{lot_type.capitalize()} tax lots CSV file is empty or invalid.")
                return False
            except pd.errors.ParserError as e:
                print(f"Error parsing {lot_type} tax lots CSV: {e}")
                return False

            # Upload data to sheet
            worksheet.clear()  # Clear existing data
            worksheet.update([df.columns.values.tolist()] + df.values.tolist())

            # format currency columns to USD
            currency_format = {
                "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}
            }
            currency_columns = [
                "costBasis",
                "unrealizedGainLoss",
                "shortTermRealizedGainLoss",
                "longTermRealizedGainLoss",
            ]
            for col in currency_columns:
                if col in df.columns:
                    col_index = df.columns.get_loc(col) + 1  # gspread is 1-indexed
                    worksheet.format(
                        f"{chr(64 + col_index)}2:{chr(64 + col_index)}{len(df)+1}",
                        currency_format,
                    )
            print(f"{sheet_title} sheet updated with data successfully.")
            return True

        except APIError as e:
            print(f"Google Sheets API error: {e}")
            return False
        except SpreadsheetNotFound:
            print(f"Spreadsheet with ID {self.spreadSheetID} not found.")
            return False
        except PermissionError as e:
            print(f"Permission error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error creating {lot_type} tax lots sheet: {e}")
            return False

    def run(self):
        """
        Main execution method that creates spreadsheet and uploads all data

        :return: True if successful, False otherwise
        """
        try:
            print("Starting Google Sheets data upload process...")
            # Authenticate Google Sheets
            self.gc = self.authenticate_google_sheets()
            if not self.gc:
                print("Google Sheets authentication failed. Aborting.")
                return False
            # Fetch spreadsheet
            sh = self.fetch_spreadsheet()
            if not sh:
                print("Failed to fetch spreadsheet. Aborting.")
                return False

            # Create or get spreadsheet
            # spreadsheet_id = self.create_spreadsheet()
            # if not spreadsheet_id:
            #     print("Failed to create or retrieve spreadsheet. Aborting.")
            #     return False

            # Create holdings sheet
            print("Creating holdings sheet...")
            holdings_created = self.create_holdings_sheet()
            if not holdings_created:
                print("Warning: Failed to create holdings sheet, but continuing...")

            # Create tax lots sheets if requested
            if self.generateTaxLotsSheets:
                print("Creating tax lots sheets...")
                open_tax_lots_created = self.create_tax_lots_sheet(lot_type="open")
                if not open_tax_lots_created:
                    print(
                        "Warning: Failed to create open tax lots sheet, but continuing..."
                    )

                closed_tax_lots_created = self.create_tax_lots_sheet(lot_type="closed")
                if not closed_tax_lots_created:
                    print(
                        "Warning: Failed to create closed tax lots sheet, but continuing..."
                    )
            else:
                print("Tax lots sheets creation skipped (generateTaxLotsSheets=False)")

            print("Google Sheets data upload process completed.")
            return True

        except Exception as e:
            print(f"Critical error in run method: {e}")
            return False
