import time
import os
from urllib import response
import requests

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive.file"]


class Authenticate:
    def __init__(self, email: str, password: str, mfaAudience: bool = False, segmentID: str = ""):
        self.email = email
        self.password = password
        self.mfaAudience = mfaAudience
        self.segmentID = segmentID

    def login(self):
        LOGIN_URL = "https://lens.m1.com/graphql"

        # Dynamic Timestamp for the sentinel header
        current_time_ms = str(int(time.time() * 1000))

        HEADERS = {
            "Host": "lens.m1.com",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://dashboard.m1.com/",
            "content-type": "application/json",
            "x-client-sentinel": current_time_ms,
            "x-segment-id": self.segmentID,
            "x-apollo-operation-name": "Authenticate",
            "Origin": "https://dashboard.m1.com",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "DNT": "1",
            "Sec-GPC": "1",
            "Priority": "u=0"
        }

        qry = """mutation Authenticate($input: AuthenticateInput!) {
  authenticate(input: $input) {
    didSucceed
    error
    outcome {
      accessToken
      refreshToken
      viewer {
        user {
          id
          correlationKey
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
    }"""

        payload = {
            "operationName": "Authenticate",
            "query": qry,
            "variables": {
                "input": {
                    "mfaAudience": self.mfaAudience,  # FIXED: Sends raw boolean false, not "false"
                    "password": self.password,
                    "username": self.email,
                }
            },
        }

        session = requests.Session()
        response = session.post(LOGIN_URL, json=payload, headers=HEADERS)
        try:
            data = response.json()
            session.access_token = data.get("data", {}).get("authenticate", {}).get("outcome", {}).get("accessToken", "")
            session.refresh_token = data.get("data", {}).get("authenticate", {}).get("outcome", {}).get("refreshToken", "")
            return session
        except Exception as e:
            print(f"Failed to authenticate: {e}")
            return None

    def auth_google_sheets(self, credentials_path="credentials.json"):
        """Shows basic usage of the Sheets API.
        Prints values from a sample spreadsheet.
        """
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_path, SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open("token.json", "w") as token:
                token.write(creds.to_json())
        return creds