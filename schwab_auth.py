import schwab
from dotenv import load_dotenv
import os

load_dotenv()

client = schwab.auth.client_from_manual_flow(
    api_key=os.getenv("SCHWAB_CLIENT_ID"),
    app_secret=os.getenv("SCHWAB_CLIENT_SECRET"),
    callback_url="https://127.0.0.1",
    token_path=os.path.expanduser("~/autonomous-trader/schwab_token.json")
)

print("✅ Auth successful! Token saved.")
