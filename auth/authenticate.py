import time
import os
from urllib import response
import requests


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
