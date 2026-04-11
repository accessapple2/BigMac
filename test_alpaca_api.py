import os
from alpaca.trading.client import TradingClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Get API credentials from environment variables
api_key = os.environ.get("ALPACA_API_KEY")
secret_key = os.environ.get("ALPACA_SECRET_KEY")

if not api_key or not secret_key:
    print("Error: ALPACA_API_KEY or ALPACA_SECRET_KEY not found. Please ensure they are set in your .env file.")
    exit(1)

try:
    # Initialize Alpaca client for paper trading
    client = TradingClient(api_key, secret_key, paper=True)
    # Test connection by getting account information
    account = client.get_account()
    print("Success: Alpaca API connection established.")
    print(f"Account Status: {account.status}")
    print(f"Account Balance: ${account.cash}")
except Exception as e:
    print(f"Error: Failed to connect to Alpaca API. {str(e)}")