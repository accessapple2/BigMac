import requests
import time
import os
from dotenv import load_dotenv
import pyotp
import logging  # Add logging import at the top

logging.basicConfig(level=logging.INFO)  # Set up logging at the beginning

load_dotenv(dotenv_path='../.env')

BASE_URL = "http://localhost:8080"
import os

USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
TOTP_SECRET = os.getenv('TOTP_SECRET')

session = requests.Session()

print("Logging in...")
r = session.post(f"{BASE_URL}/login", data={"username": USERNAME, "password": PASSWORD})
if r.status_code != 200:
    raise ValueError(f"Login failed with status code {r.status_code}")
print("Login status:", r.status_code)

if r.status_code == 200:
    totp = pyotp.TOTP(TOTP_SECRET)
    code = totp.now()
r = session.post(f"{BASE_URL}/2fa", json={"totp": code})
if r.status_code != 200:
    raise ValueError(f"2FA failed with status code {r.status_code}")

endpoints = [
    "/", "/dashboard", "/api/me", "/api/active-users", 
    "/api/charts/ping", "/api/bridge/votes", "/api/bridge/consensus", "/setup-2fa"
]

print("\nTesting GET endpoints:\n")
def test_endpoints(session):
    for ep in endpoints:
        start = time.time()
        try:
            r = session.get(f"{BASE_URL}{ep}", timeout=5)
            t = time.time() - start
            status = "OK" if r.status_code == 200 and t <= 3 else "Flagged"
            logging.info(f"{ep:30} {r.status_code:3}  {t:.2f}s   {status}")
        except Exception as e:
            print(f"{ep:30} Error   {str(e)[:30]}")

    print("\nDone.")

if __name__ == "__main__":
    test_endpoints(session)
