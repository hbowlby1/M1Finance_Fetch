'''
This module just checks to see if a state.json file exists
and if it doesn't then it creates a default one.
'''
import json
import os

STATE_FILE = "state.json"
DEFAULT_STATE = {
    "ENABLE_GOOGLE_SHEETS_INTEGRATION": True,
    "CREATE_NEW_SPREADSHEET": False,
    "SPREADSHEET_NAME": "M1 Finance Management",
    "CREATE_CSV_FILES": True,
    "GENERATE_TAX_LOTS_SHEETS": True
}

def check_for_state_file():
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, "w") as state_file:
            json.dump(DEFAULT_STATE, state_file, indent=4)
        print(f"{STATE_FILE} created with default settings.")
    else:
        print(f"{STATE_FILE} already exists.")