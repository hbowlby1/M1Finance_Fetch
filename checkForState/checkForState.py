'''
This module just checks to see if a state.json file exists
and if it doesn't then it creates a default one.
'''
import json
import os

STATE_FILE = "state.json"
DEFAULT_STATE = {
    "ENABLE_GOOGLE_SHEETS_INTEGRATION": False,
    "CREATE_NEW_SPREADSHEET": False,
    "SPREADSHEET_NAME": "M1 Finance Management",
    "CREATE_CSV_FILES": True,
    "GENERATE_TAX_LOTS_SHEETS": True,
    "USE_DATABASE": False,
}

def check_for_state_file(state_file_path=STATE_FILE):
    if not os.path.exists(state_file_path):
        with open(state_file_path, "w") as state_file:
            json.dump(DEFAULT_STATE, state_file, indent=4)
        print(f"{state_file_path} created with default settings.")
    else:
        print(f"{state_file_path} already exists.")
