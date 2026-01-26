'''
This module just checks to see if a state.json file exists
and if it doesn't then it creates a default one.
'''
import json
import os
import logging

logger = logging.getLogger(__name__)

STATE_FILE = "state.json"
DEFAULT_STATE = {
    "ENABLE_GOOGLE_SHEETS_INTEGRATION": False,
    "CREATE_NEW_SPREADSHEET": False,
    "SPREADSHEET_NAME": "M1 Finance Management",
    "CREATE_CSV_FILES": True,
    "GENERATE_TAX_LOTS_SHEETS": True,
    "USE_DATABASE": False,
    "USE_LOGGING": True,
    "LOG_FILE_NAME": "app.log"
}

def check_for_state_file(state_file_path=STATE_FILE):
    if not os.path.exists(state_file_path):
        with open(state_file_path, "w") as state_file:
            json.dump(DEFAULT_STATE, state_file, indent=4)
        logger.info("%s created with default settings.", state_file_path)
    else:
        logger.info("%s already exists.", state_file_path)
