from dotenv import load_dotenv
from auth.authenticate import Authenticate
import os

load_dotenv()
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")
MFAAUDIENCE = os.getenv("MFA_AUDIENCE", "false").lower() == "true"

if __name__ == "__main__":
    auth = Authenticate(EMAIL, PASSWORD, MFAAUDIENCE)
    auth.login()