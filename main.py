from dotenv import load_dotenv
from auth.authenticate import Authenticate
from fetch_csv.fetch_csv import FetchCSV
import os
from generateCSV.generateCSV import GenerateCSV
import json
from checkForState import check_for_state_file
from spreadsheets.spreadsheetManager import spreadsheetManager

STATE_FILE = "state.json"
with open(STATE_FILE, "r") as state_file:
    state_data = json.load(state_file)

load_dotenv()
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")
MFAAUDIENCE = os.getenv("MFA_AUDIENCE", "false").lower() == "true"
SEGMENTID = os.getenv("SEGMENT_ID", "")
ACCOUNTID = os.getenv("ACCOUNT_ID", "")
OTHERACCOUNTID = os.getenv("OTHER_ACCOUNT_ID", "")
ENABLE_GOOGLE_SHEETS_INTEGRATION = state_data.get("ENABLE_GOOGLE_SHEETS_INTEGRATION", False)
CREDENTIALS_PATH = os.path.join(os.getcwd(), "serviceAccount.json")
CREATE_NEW_SPREADSHEET = state_data.get("CREATE_NEW_SPREADSHEET", False)
CREATE_CSV_FILES = state_data.get("CREATE_CSV_FILES", False)
GENERATE_TAX_LOTS_SHEETS = state_data.get("GENERATE_TAX_LOTS_SHEETS", False)
SPREADSHEET_NAME = state_data.get("SPREADSHEET_NAME", "M1 Finance Data")

def fetchM1Data():
    auth = Authenticate(EMAIL, PASSWORD, MFAAUDIENCE, SEGMENTID)
    auth_session = auth.login()
    creds = None
    # generate Google Sheets credentials 
    if ENABLE_GOOGLE_SHEETS_INTEGRATION:
        creds = auth.auth_google_sheets(credentials_path=CREDENTIALS_PATH)
        if creds:
            print("Google Sheets authentication successful.")
        else:
            print("Google Sheets authentication failed.")
    if auth_session:
        fetcher = FetchCSV(auth_session, SEGMENTID, OTHERACCOUNTID)
        openTaxLots, closedTaxLots = fetcher.fetchTaxLotsCSVs()
        #save openTaxLots to CSV
        if openTaxLots is not None:
            GenerateCSV_instance = GenerateCSV(openTaxLots)
            if CREATE_CSV_FILES:
                GenerateCSV_instance.save_to_csv("open_tax_lots.csv")
            else:
                print("Skipping CSV generation for open tax lots as per configuration.")
        #save closedTaxLots to CSV
        if closedTaxLots is not None:
            GenerateCSV_instance = GenerateCSV(closedTaxLots)
            if CREATE_CSV_FILES:
                GenerateCSV_instance.save_to_csv("closed_tax_lots.csv")
            else:
                print("Skipping CSV generation for closed tax lots as per configuration.")
        #fetch holdings
        holdings = fetcher.fetchHoldingsCSV()
        #save holdings to CSV
        if holdings is not None:
            GenerateCSV_instance = GenerateCSV(holdings)
            if CREATE_CSV_FILES:
                GenerateCSV_instance.save_to_csv("holdings.csv")
            else:
                print("Skipping CSV generation for holdings as per configuration.")
        return creds
    else:
        print("Authentication failed.")
        

if __name__ == "__main__":
    check_for_state_file()
    creds = fetchM1Data()
    if creds and ENABLE_GOOGLE_SHEETS_INTEGRATION:
        sheet_manager = spreadsheetManager(spreadsheetName=SPREADSHEET_NAME,
                                           credentialsPath = CREDENTIALS_PATH, 
                                           CSVFolderPath="CSV", 
                                           createNewSpreadSheet=CREATE_NEW_SPREADSHEET, 
                                           generateTaxLotsSheets=GENERATE_TAX_LOTS_SHEETS)
        sheet_manager.run()