import time
import os
from urllib import response
import requests


class Authenticate:
    def __init__(self, email: str, password: str, mfaAudience: bool = False):
        self.email = email
        self.password = password
        self.mfaAudience = mfaAudience

    def login(self):
        LOGIN_URL = "https://lens.m1.com/graphql"

        # Dynamic Timestamp for the sentinel header
        current_time_ms = str(int(time.time() * 1000))

        HEADERS = {
            "Host": "lens.m1.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) Gecko/20100101 Firefox/146.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://dashboard.m1.com/",
            "content-type": "application/json",
            "x-client-id": "m1-web/10.0.170",
            "x-client-sentinel": current_time_ms,  # UPDATED: Now dynamic
            "x-segment-id": "0c040cbf-d04d-48f8-893f-0cf4cc877499",  # Note: This might eventually expire
            "x-client-timezone": "America/Phoenix",
            "x-apollo-operation-name": "Authenticate",
            "Origin": "https://dashboard.m1.com",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "DNT": "1",
            "Sec-GPC": "1",
            "Priority": "u=0",
            # "TE": "trailers", # Optional, requests usually handles this
        }

        # FIXED: Removed the extra quote at the start
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

        response = requests.post(LOGIN_URL, json=payload, headers=HEADERS)

        print(f"Status: {response.status_code}")
        try:
            print(response.json())
        except:
            print(response.text)
