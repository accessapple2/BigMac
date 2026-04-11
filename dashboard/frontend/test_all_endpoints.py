import requests
import time
from dotenv import load_dotenv
import os
import pyotp
import tabulate  # Assuming tabulate is installed for nice tables

# Load environment variables
load_dotenv()
USERNAME = "Sniff"
PASSWORD = os.getenv("PASSWORD")  # From .env
TOTP_SECRET = os.getenv("TOTP_SECRET")  # From .env

# List of GET endpoints from app.py
GET_ENDPOINTS = ["/", "/login", "/dashboard"]

session = requests.Session()

def login():
    # Step 1: Initial login to get TOTP challenge (assuming it returns a challenge or directly handles it)
    response = session.post("http://localhost:8080/login", data={"username": USERNAME, "password": PASSWORD})
    if response.status_code == 200:
        # Assuming TOTP is needed next
        totp = pyotp.TOTP(TOTP_SECRET)
        otp_code = totp.now()
        # Send TOTP code; adjust based on actual endpoint
        response = session.post("http://localhost:8080/2fa", data={"otp": otp_code})  # Hypothetical 2FA endpoint
        return response.status_code == 200
    return False

if not login():
    print("Login failed")
    exit(1)

results = []

for endpoint in GET_ENDPOINTS:
    start_time = time.time()
    try:
        response = session.get(f"http://localhost:8080{endpoint}")
        end_time = time.time()
        duration = end_time - start_time
        status = response.status_code
        flagged = status != 200 or duration > 3
        results.append([endpoint, status, f"{duration:.2f} seconds", "Flagged" if flagged else "OK"])
    except Exception as e:
        results.append([endpoint, "Error", "N/A", "Flagged"])

# Print summary table
headers = ["Endpoint", "Status Code", "Time Taken", "Status"]
print(tabulate.tabulate(results, headers=headers))