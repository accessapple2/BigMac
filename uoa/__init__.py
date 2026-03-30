"""
TradeMinds UOA (Unusual Options Activity) Module
================================================
Scans options chains for unusual volume, big premium bets,
and smart money flow using FREE data sources.

Sources:
  - yfinance (primary) - free options chains with Vol/OI
  - CBOE delayed data (secondary)
  - Barchart unofficial API (supplementary)

Integration:
  - Stores all data in trader.db (sacred, never delete)
  - Feeds alerts to Chekov scanner + War Room
  - CrewAI Scout can use UOA alerts as a tool
"""

__version__ = "1.0.0"
