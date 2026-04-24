"""
Strategy registry. Single source of truth for which strategies exist
and which are enabled.
"""
from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Optional
from .base import Strategy, MarketContext, StrategySignal


DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


class StrategyRegistry:
    def __init__(self):
        self._strategies: dict[str, Strategy] = {}

    def register(self, strategy: Strategy) -> None:
        if strategy.strategy_id in self._strategies:
            raise ValueError(f"Strategy {strategy.strategy_id} already registered")
        self._strategies[strategy.strategy_id] = strategy
        self._sync_db(strategy)

    def get(self, strategy_id: str) -> Optional[Strategy]:
        return self._strategies.get(strategy_id)

    def all(self) -> list[Strategy]:
        return list(self._strategies.values())

    def enabled(self) -> list[Strategy]:
        return [s for s in self._strategies.values() if s.is_enabled()]

    def evaluate_all(self, ctx: MarketContext) -> list[StrategySignal]:
        signals: list[StrategySignal] = []
        for s in self.enabled():
            try:
                signals.extend(s.evaluate(ctx))
            except Exception as e:
                # Log but don't let one bad strategy kill the run
                print(f"[registry] {s.strategy_id} evaluate() raised {type(e).__name__}: {e}")
        return signals

    def enable(self, strategy_id: str) -> bool:
        s = self._strategies.get(strategy_id)
        if s is None:
            return False
        s.enabled = True
        self._sync_db(s)
        return True

    def disable(self, strategy_id: str) -> bool:
        s = self._strategies.get(strategy_id)
        if s is None:
            return False
        s.enabled = False
        self._sync_db(s)
        return True

    def _sync_db(self, strategy: Strategy) -> None:
        """Upsert strategy state into DB for dashboard visibility."""
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute("""
                INSERT INTO strategies (strategy_id, display_name, enabled, description)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(strategy_id) DO UPDATE SET
                    display_name=excluded.display_name,
                    enabled=excluded.enabled,
                    description=excluded.description
            """, (strategy.strategy_id, strategy.display_name,
                  int(strategy.enabled), strategy.description))
            conn.commit()
        except sqlite3.OperationalError as e:
            # Table may not exist yet during initial migration
            print(f"[registry] DB sync skipped: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass


# Module-level singleton
_registry = StrategyRegistry()


def registry() -> StrategyRegistry:
    return _registry
