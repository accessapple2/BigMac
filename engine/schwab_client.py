import schwab
from dotenv import load_dotenv
import os

load_dotenv()

_client = None

def get_client():
    global _client
    if _client is None:
        _client = schwab.auth.client_from_token_file(
            token_path=os.path.expanduser("~/autonomous-trader/schwab_token.json"),
            api_key=os.getenv("SCHWAB_CLIENT_ID"),
            app_secret=os.getenv("SCHWAB_CLIENT_SECRET")
        )
    return _client

def get_account_numbers():
    c = get_client()
    r = c.get_account_numbers()
    return r.json()

def get_accounts():
    c = get_client()
    r = c.get_accounts()
    return r.json()

if __name__ == "__main__":
    print("Account numbers:", get_account_numbers())
