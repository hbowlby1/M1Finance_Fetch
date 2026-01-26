"""
Creates or manages the spreadsheets containing the user's M1 Finance
data using the Google Sheets API mixed with gsheets
"""

import os
import logging
import gspread
import json
import pandas as pd
import numpy as np
import requests
import yfinance as yf
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound, APIError

logger = logging.getLogger(__name__)


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
            logger.error("Initialization error: %s", e)
            raise
        except Exception:
            logger.exception("Unexpected error during initialization.")
            raise

    def authenticate_google_sheets(self):
        """
        Authenticates and returns a gspread client

        :return: gspread Client object or None if failed
        """
        try:
            # Validate credentials file exists
            if not os.path.exists(self.credentialsPath):
                logger.error("Credentials file not found at %s", self.credentialsPath)
                return None

            # Initialize gspread client
            self.gc = gspread.service_account(filename=self.credentialsPath)
            return self.gc

        except Exception:
            logger.exception("Google Sheets authentication error.")
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
            logger.warning("Spreadsheet named '%s' not found.", self.spreadsheetName)
            return None
        except Exception:
            logger.exception("Error fetching spreadsheet.")
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
                logger.error("Spreadsheet ID is not set. Please create a spreadsheet first.")
                return False

            # Validate CSV folder exists
            if not os.path.exists(self.CSVFolderPath):
                logger.error("CSV folder not found at %s", self.CSVFolderPath)
                return False

            # Open spreadsheet
            sh = self.gc.open_by_key(self.SpreadSheetID)

            # Check if holdings sheet already exists
            try:
                worksheet = sh.worksheet("Holdings")
                logger.info("Holdings sheet already exists. Updating data...")
            except WorksheetNotFound:
                worksheet = sh.add_worksheet(title="Holdings", rows="100", cols="20")
                logger.info("Created new Holdings worksheet.")

            # Validate and read CSV file
            holdings_csv_path = os.path.join(self.CSVFolderPath, "holdings.csv")
            if not os.path.exists(holdings_csv_path):
                logger.error("Holdings CSV file not found at %s", holdings_csv_path)
                return False

            try:
                df = pd.read_csv(holdings_csv_path)
                if df.empty:
                    logger.warning("Holdings CSV file is empty.")
                    return False
                # set columns to respective types and if null or NaN set to None
                df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").where(pd.notnull(df["quantity"]), None)
                # currency columns
                df["average_share_price"] = pd.to_numeric(
                    df["average_share_price"], errors="coerce"
                ).where(pd.notnull(df["average_share_price"]), None)
                df["total_cost"] = pd.to_numeric(df["total_cost"], errors="coerce").where(pd.notnull(df["total_cost"]), None)
                df["current_value"] = pd.to_numeric(
                    df["current_value"], errors="coerce"
                ).where(pd.notnull(df["current_value"]), None)
                df["unrealized_gain"] = pd.to_numeric(
                    df["unrealized_gain"], errors="coerce"
                ).where(pd.notnull(df["unrealized_gain"]), None)
                # percentage columns
                df["unrealized_gain_percent"] = (
                    pd.to_numeric(df["unrealized_gain_percent"], errors="coerce") / 100
                ).where(pd.notnull(df["unrealized_gain_percent"]), None)
                df["maintenance_margin_percent"] = (
                    pd.to_numeric(df["maintenance_margin_percent"], errors="coerce")
                    / 100
                ).where(pd.notnull(df["maintenance_margin_percent"]), None)
                # Replace any remaining NaN with empty string for JSON compatibility
                df = df.fillna('')
            except pd.errors.EmptyDataError:
                logger.error("Holdings CSV file is empty or invalid.")
                return False
            except pd.errors.ParserError:
                logger.exception("Error parsing holdings CSV.")
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
            logger.info("Holdings sheet updated with data successfully.")
            return True

        except APIError:
            logger.exception("Google Sheets API error.")
            return False
        except SpreadsheetNotFound:
            logger.error("Spreadsheet with ID %s not found.", self.SpreadSheetID)
            return False
        except PermissionError:
            logger.exception("Permission error.")
            return False
        except Exception:
            logger.exception("Unexpected error creating holdings sheet.")
            return False

    def create_tax_lots_sheet(self, lot_type="open"):
        """
        Creates a tax lots worksheet and uploads tax lots data from CSV

        :param lot_type: Type of tax lots ("open" or "closed")
        :return: True if successful, False otherwise
        """
        try:
            if not self.SpreadSheetID:
                logger.error("Spreadsheet ID is not set. Please create a spreadsheet first.")
                return False

            # Validate lot_type parameter
            if lot_type not in ["open", "closed"]:
                logger.error("Invalid lot_type: %s. Must be 'open' or 'closed'.", lot_type)
                return False

            # Validate CSV folder exists
            if not os.path.exists(self.CSVFolderPath):
                logger.error("CSV folder not found at %s", self.CSVFolderPath)
                return False

            # Open spreadsheet
            sh = self.gc.open_by_key(self.SpreadSheetID)

            # Determine sheet title
            sheet_title = "Open Tax Lots" if lot_type == "open" else "Closed Tax Lots"

            # Check if tax lots sheet already exists
            try:
                worksheet = sh.worksheet(sheet_title)
                logger.info("%s sheet already exists. Updating data...", sheet_title)
            except WorksheetNotFound:
                worksheet = sh.add_worksheet(title=sheet_title, rows="100", cols="20")
                logger.info("Created new %s worksheet.", sheet_title)

            # Validate and read CSV file
            tax_lots_csv_path = os.path.join(
                self.CSVFolderPath, f"{lot_type}_tax_lots.csv"
            )
            if not os.path.exists(tax_lots_csv_path):
                logger.error(
                    "%s tax lots CSV file not found at %s",
                    lot_type.capitalize(),
                    tax_lots_csv_path,
                )
                return False

            try:
                df = pd.read_csv(tax_lots_csv_path)
                if df.empty:
                    logger.warning("%s tax lots CSV file is empty.", lot_type.capitalize())
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
                logger.error("%s tax lots CSV file is empty or invalid.", lot_type.capitalize())
                return False
            except pd.errors.ParserError:
                logger.exception("Error parsing %s tax lots CSV.", lot_type)
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
            logger.info("%s sheet updated with data successfully.", sheet_title)
            return True

        except APIError:
            logger.exception("Google Sheets API error.")
            return False
        except SpreadsheetNotFound:
            logger.error("Spreadsheet with ID %s not found.", self.SpreadSheetID)
            return False
        except PermissionError:
            logger.exception("Permission error.")
            return False
        except Exception:
            logger.exception("Unexpected error creating %s tax lots sheet.", lot_type)
            return False
        
    # method that generates a securities info sheet to track types of securities
    def create_securities_info_sheet(self):
        """
        Creates a securities info worksheet and uploads securities data from CSV that places the data in the sheet
        The sheet will need to be manually set up with the graphs.

        :return: True if successful, False otherwise
        """
        try:
            if not self.SpreadSheetID:
                logger.error("Spreadsheet ID is not set. Please create a spreadsheet first.")
                return False

            # fetch the holdings spreadsheet (verify it exists first)
            sh = self.gc.open_by_key(self.SpreadSheetID)
            try:
                sec_worksheet = sh.worksheet("Securities Info")
                logger.info("Securities Info sheet exists. Updating data...")
            except WorksheetNotFound:
                sec_worksheet = sh.add_worksheet(
                    title="Securities Info", rows="100", cols="20"
                )
                logger.info("Created new Securities Info worksheet.")
            try:
                holdings_worksheet = sh.worksheet("Holdings")
                # grab the symbol and current value columns from holdings
                holdings_data = pd.DataFrame(holdings_worksheet.get_all_records())
                if holdings_data.empty:
                    logger.warning("Holdings sheet is empty. Cannot create Securities Info sheet.")
                    return None
                securities_info = holdings_data[["symbol", "current_value"]].copy()
                # remove holdings with zero current value or is null or NaN
                securities_info = securities_info[
                    (securities_info["current_value"].notnull())
                    & (securities_info["current_value"] != 0)
                ]
                # return the holding information to be passed to the next method for generating new security column
                return securities_info
            except WorksheetNotFound:
                logger.error("Could not find Holdings sheet. Cannot create Securities Info sheet.")
                return None
                
        except APIError:
            logger.exception("Google Sheets API error.")
            return None
        except SpreadsheetNotFound:
            logger.error("Spreadsheet with ID %s not found.", self.SpreadSheetID)
            return None
        except PermissionError:
            logger.exception("Permission error.")
            return None
        except Exception:
            logger.exception("Unexpected error creating securities info sheet.")
            return None
        
    def generate_securities_type_column(self, securities_info_df):
        """
        Generates a 'security_type' column in the securities info DataFrame
        by fetching data from Yahoo Finance API.

        :param securities_info_df: DataFrame with 'symbol' and 'current_value' columns
        :return: DataFrame with added 'security_type' column
        """
        try:
            if securities_info_df is None:
                logger.error("Securities info DataFrame is None. Cannot generate security types.")
                return None
            if securities_info_df.empty:
                logger.warning("Securities info DataFrame is empty. Cannot generate security types.")
                return securities_info_df

            def fetch_security_type(symbol):
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.info
                    return info.get("quoteType", "Unknown")
                except Exception:
                    logger.exception("Error fetching data for symbol %s.", symbol)
                    return "Unknown"

            securities_info_df["security_type"] = securities_info_df["symbol"].apply(fetch_security_type)
            logger.info("Security types fetched and added to DataFrame.")
            return securities_info_df

        except Exception:
            logger.exception("Unexpected error generating security types.")
            return securities_info_df
        
    def combine_securities_info_with_sheet(self, securities_info_df):
        """
        Combines the securities info DataFrame with the Securities Info sheet in Google Sheets.

        :param securities_info_df: DataFrame with 'symbol', 'current_value', and 'security_type' columns
        :return: True if successful, False otherwise
        """
        try:
            if not self.SpreadSheetID:
                logger.error("Spreadsheet ID is not set. Please create a spreadsheet first.")
                return False
            
            if securities_info_df is None:
                logger.error("Securities info DataFrame is None. Cannot update sheet.")
                return False
            if securities_info_df.empty:
                logger.warning("Securities info DataFrame is empty. Cannot update sheet.")
                return False

            # Open spreadsheet
            sh = self.gc.open_by_key(self.SpreadSheetID)
            sec_worksheet = sh.worksheet("Securities Info")

            # Upload data to sheet
            sec_worksheet.clear()  # Clear existing data
            sec_worksheet.update([securities_info_df.columns.values.tolist()] + securities_info_df.values.tolist())

            logger.info("Securities Info sheet updated with %s rows.", len(securities_info_df))
            return True

        except APIError:
            logger.exception("Google Sheets API error.")
            return False
        except SpreadsheetNotFound:
            logger.error("Spreadsheet with ID %s not found.", self.SpreadSheetID)
            return False
        except PermissionError:
            logger.exception("Permission error.")
            return False
        except Exception:
            logger.exception("Unexpected error updating securities info sheet.")
            return False

    def run(self):
        """
        Main execution method that creates spreadsheet and uploads all data

        :return: True if successful, False otherwise
        """
        try:
            logger.info("Starting Google Sheets data upload process...")
            # Authenticate Google Sheets
            self.gc = self.authenticate_google_sheets()
            if not self.gc:
                logger.error("Google Sheets authentication failed. Aborting.")
                return False
            # Fetch spreadsheet
            sh = self.fetch_spreadsheet()
            if not sh:
                logger.error("Failed to fetch spreadsheet. Aborting.")
                return False

            # Create or get spreadsheet
            # spreadsheet_id = self.create_spreadsheet()
            # if not spreadsheet_id:
            #     print("Failed to create or retrieve spreadsheet. Aborting.")
            #     return False

            # Create holdings sheet
            logger.info("Creating holdings sheet...")
            holdings_created = self.create_holdings_sheet()
            if not holdings_created:
                logger.warning("Failed to create holdings sheet, but continuing...")

            # Create tax lots sheets if requested
            if self.generateTaxLotsSheets:
                logger.info("Creating tax lots sheets...")
                open_tax_lots_created = self.create_tax_lots_sheet(lot_type="open")
                if not open_tax_lots_created:
                    logger.warning("Failed to create open tax lots sheet, but continuing...")

                closed_tax_lots_created = self.create_tax_lots_sheet(lot_type="closed")
                if not closed_tax_lots_created:
                    logger.warning("Failed to create closed tax lots sheet, but continuing...")
            else:
                logger.info("Tax lots sheets creation skipped (generateTaxLotsSheets=False)")
                
            # create securities info sheet
            logger.info("Creating securities info sheet...")
            securities_info_created = self.create_securities_info_sheet()
            if securities_info_created is None or securities_info_created.empty:
                logger.warning("Failed to create securities info sheet, but continuing...")
            else:
                # generate securities type column
                securities_info_df = self.generate_securities_type_column(securities_info_created)
                # combine with sheet
                combined_success = self.combine_securities_info_with_sheet(securities_info_df)
                if not combined_success:
                    logger.warning("Failed to update securities info sheet with data, but continuing...")

            logger.info("Google Sheets data upload process completed.")
            return True

        except Exception:
            logger.exception("Critical error in run method.")
            return False
