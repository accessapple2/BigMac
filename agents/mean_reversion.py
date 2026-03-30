class MeanReversionAgent:
    def __init__(self):
        self.name = "MeanReversion"

    def scan(self, market_data):
        signals = []

        for symbol, data in market_data.items():
            if len(data) < 5:
                continue

            current = data[-1]
            avg_price = sum(data[-5:]) / 5

            deviation = (current - avg_price) / avg_price

            if deviation < -0.02:
                signals.append({
                    "symbol": symbol,
                    "action": "BUY",
                    "confidence": round(abs(deviation), 2),
                    "reason": "mean reversion"
                })

        return signals