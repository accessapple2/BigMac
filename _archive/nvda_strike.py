Python
import uuid
from webullsdktrade.api import API
from webullsdkcore.client import ApiClient
from webullsdkcore.common.region import Region

# --- GENERAL'S CREDENTIALS ---
APP_KEY = "YOUR_APP_KEY_HERE"
APP_SECRET = "YOUR_APP_SECRET_HERE"
ACCOUNT_ID = "YOUR_ACCOUNT_ID_HERE"

# 1. Initialize the Command Bridge
client = ApiClient(APP_KEY, APP_SECRET, Region.US.value)
webull_api = API(client)

# 2. Define the Mission: NVDA Re-Entry at $181.50
nvda_mission = {
    "account_id": ACCOUNT_ID,
    "qty": "12",
    "instrument_id": "913256135", # NVDA Verified ID
    "side": "BUY",
    "order_type": "LIMIT",
    "limit_price": "181.50",
    "tif": "DAY",
    "extended_hours_trading": True,
    "client_order_id": str(uuid.uuid4())
}

# 3. Execute the Strike
print("🚀 Launching NVDA Buy Limit Order...")
response = webull_api.order.place_order(**nvda_mission)

if response.status_code == 200:
    print(f"✅ MISSION SUCCESS: Order placed. ID: {response.json().get('order_id')}")
else:
    print(f"❌ MISSION FAILURE: {response.text}")