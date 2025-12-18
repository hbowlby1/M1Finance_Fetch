from dotenv import load_dotenv
from auth.authenticate import Authenticate
from fetch_csv.fetch_csv import FetchCSV
import os
from generateCSV.generateCSV import GenerateCSV

load_dotenv()
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")
MFAAUDIENCE = os.getenv("MFA_AUDIENCE", "false").lower() == "true"
SEGMENTID = os.getenv("SEGMENT_ID", "")
ACCOUNTID = os.getenv("ACCOUNT_ID", "")
OTHERACCOUNTID = os.getenv("OTHER_ACCOUNT_ID", "")

if __name__ == "__main__":
    auth = Authenticate(EMAIL, PASSWORD, MFAAUDIENCE, SEGMENTID)
    auth_session = auth.login()
    if auth_session:
        fetcher = FetchCSV(auth_session, SEGMENTID, OTHERACCOUNTID)
        openTaxLots, closedTaxLots = fetcher.fetchTaxLotsCSVs()
        #save openTaxLots to CSV
        if openTaxLots is not None:
            GenerateCSV_instance = GenerateCSV(openTaxLots)
            GenerateCSV_instance.save_to_csv("open_tax_lots.csv")
        #save closedTaxLots to CSV
        if closedTaxLots is not None:
            GenerateCSV_instance = GenerateCSV(closedTaxLots)
            GenerateCSV_instance.save_to_csv("closed_tax_lots.csv")
        
    else:
        print("Authentication failed.")