class MomentumAgent:
    def __init__(self):
        self.name = "Momentum"

    def scan(self, market_data):
        signals = []

        for symbol, data in market_data.items():
            if len(data) < 5:
                continue

            recent = data[-1]
            prev = data[-5]

            change = (recent - prev) / prev

            if change > 0.02:
                signals.append({
                    "symbol": symbol,
                    "action": "BUY",
                    "confidence": round(change, 2),
                    "reason": "momentum"
                })

        return signals