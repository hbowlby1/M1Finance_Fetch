from dotenv import load_dotenv
from auth.authenticate import Authenticate
from fetch_csv.fetch_csv import FetchCSV
import os
from generateCSV.generateCSV import GenerateCSV
import json
from checkForState import check_for_state_file
from spreadsheets.spreadsheetManager import spreadsheetManager
# from database.database_setup import Asset coming soon

CONFIG_DIR = os.getenv("CONFIG_DIR", os.path.join(os.getcwd(), "config"))
STATE_FILE = os.path.join(CONFIG_DIR, "state.json")
ENV_FILE = os.path.join(CONFIG_DIR, ".env")
SERVICE_ACCOUNT_FILE = os.path.join(CONFIG_DIR, "serviceAccount.json")

ENV_TEMPLATE = [
    "EMAIL=",
    "PASSWORD=",
    "MFA_AUDIENCE=false",
    "SEGMENT_ID=",
    "ACCOUNT_ID=",
    "OTHER_ACCOUNT_ID=",
]

SERVICE_ACCOUNT_TEMPLATE = {
  "type": "placeholder",
  "project_id": "placeholder",
  "private_key_id": "placeholder",
  "private_key": "placeholder",
  "client_email": "placeholder",
  "client_id": "placeholder",
  "auth_uri": "placeholder",
  "token_uri": "placeholder",
  "auth_provider_x509_cert_url": "placeholder",
  "client_x509_cert_url": "placeholder",
  "universe_domain": "placeholder"
}

def ensure_env_file():
    os.makedirs(CONFIG_DIR, exist_ok=True)
    if not os.path.exists(ENV_FILE):
        with open(ENV_FILE, "w", encoding="utf-8") as env_file:
            env_file.write(
                "# Fill in your settings, then re-run the app.\n"
                "# EMAIL and PASSWORD are required for login.\n"
            )
            env_file.write("\n".join(ENV_TEMPLATE))
            env_file.write("\n")
        print(f"{ENV_FILE} created with placeholder settings.")
    else:
        print(f"{ENV_FILE} already exists.")
        
def ensure_service_account_file():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        with open(SERVICE_ACCOUNT_FILE, "w", encoding="utf-8") as sa_file:
            json.dump(SERVICE_ACCOUNT_TEMPLATE, sa_file, indent=4)
        print(f"{SERVICE_ACCOUNT_FILE} created with placeholder settings.")
    else:
        print(f"{SERVICE_ACCOUNT_FILE} already exists.")
    


def load_state_data():
    os.makedirs(CONFIG_DIR, exist_ok=True)
    check_for_state_file(STATE_FILE)
    with open(STATE_FILE, "r") as state_file:
        return json.load(state_file)

state_data = load_state_data()
ensure_env_file()
ensure_service_account_file()
load_dotenv(ENV_FILE)
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")
MFAAUDIENCE = os.getenv("MFA_AUDIENCE", "false").lower() == "true"
SEGMENTID = os.getenv("SEGMENT_ID", "")
ACCOUNTID = os.getenv("ACCOUNT_ID", "")
OTHERACCOUNTID = os.getenv("OTHER_ACCOUNT_ID", "")
ENABLE_GOOGLE_SHEETS_INTEGRATION = state_data.get("ENABLE_GOOGLE_SHEETS_INTEGRATION", False)
CREDENTIALS_PATH = os.path.join(CONFIG_DIR, "serviceAccount.json")
CREATE_NEW_SPREADSHEET = state_data.get("CREATE_NEW_SPREADSHEET", False)
CREATE_CSV_FILES = state_data.get("CREATE_CSV_FILES", False)
GENERATE_TAX_LOTS_SHEETS = state_data.get("GENERATE_TAX_LOTS_SHEETS", False)
SPREADSHEET_NAME = state_data.get("SPREADSHEET_NAME", "M1 Finance Data")
USE_DATABASE = state_data.get("USE_DATABASE", False)

def fetchM1Data():
    try:
        auth = Authenticate(EMAIL, PASSWORD, MFAAUDIENCE, SEGMENTID)
        auth_session = auth.login()
        creds = None
        # generate Google Sheets credentials 
        if ENABLE_GOOGLE_SHEETS_INTEGRATION:
            try:
                creds = auth.auth_google_sheets(credentials_path=CREDENTIALS_PATH)
                if creds:
                    print("Google Sheets authentication successful.")
                else:
                    print("Google Sheets authentication failed.")
            except Exception as e:
                print(f"Error during Google Sheets authentication: {e}")
                creds = None
        if auth_session:
            try:
                fetcher = FetchCSV(auth_session, SEGMENTID, OTHERACCOUNTID)
                openTaxLots, closedTaxLots = fetcher.fetchTaxLotsCSVs()
                #save openTaxLots to CSV
                if openTaxLots is not None:
                    try:
                        GenerateCSV_instance = GenerateCSV(openTaxLots)
                        if CREATE_CSV_FILES:
                            GenerateCSV_instance.save_to_csv("open_tax_lots.csv")
                        else:
                            print("Skipping CSV generation for open tax lots as per configuration.")
                    except Exception as e:
                        print(f"Error saving open tax lots CSV: {e}")
                #save closedTaxLots to CSV
                if closedTaxLots is not None:
                    try:
                        GenerateCSV_instance = GenerateCSV(closedTaxLots)
                        if CREATE_CSV_FILES:
                            GenerateCSV_instance.save_to_csv("closed_tax_lots.csv")
                        else:
                            print("Skipping CSV generation for closed tax lots as per configuration.")
                    except Exception as e:
                        print(f"Error saving closed tax lots CSV: {e}")
                #fetch holdings
                holdings = fetcher.fetchHoldingsCSV()
                #save holdings to CSV
                if holdings is not None:
                    try:
                        GenerateCSV_instance = GenerateCSV(holdings)
                        if CREATE_CSV_FILES:
                            GenerateCSV_instance.save_to_csv("holdings.csv")
                        else:
                            print("Skipping CSV generation for holdings as per configuration.")
                    except Exception as e:
                        print(f"Error saving holdings CSV: {e}")
                return creds
            except Exception as e:
                print(f"Error during data fetching: {e}")
                return None
        else:
            print("Authentication failed.")
            return None
    except Exception as e:
        print(f"Unexpected error in fetchM1Data: {e}")
        return None
        

if __name__ == "__main__":
    try:
        creds = fetchM1Data()
        #check and initialize database coming soon
        # if USE_DATABASE:
        #     Asset.init_db()
        #     #insert assets from generated CSVs into database
        #     Asset.insert_asset()
        if creds and ENABLE_GOOGLE_SHEETS_INTEGRATION:
            try:
                sheet_manager = spreadsheetManager(spreadsheetName=SPREADSHEET_NAME,
                                                   credentialsPath = CREDENTIALS_PATH, 
                                                   CSVFolderPath="CSV", 
                                                   createNewSpreadSheet=CREATE_NEW_SPREADSHEET, 
                                                   generateTaxLotsSheets=GENERATE_TAX_LOTS_SHEETS)
                sheet_manager.run()
            except Exception as e:
                print(f"Error in spreadsheet management: {e}")
    except Exception as e:
        print(f"Error in main execution: {e}")
