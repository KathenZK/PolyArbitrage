from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.binance_stream import BinanceStream, Tick
from src.data.market_registry import MarketRegistry
from src.data.polymarket_client import PolymarketGammaClient

logger = logging.getLogger("snapshot_recorder")


class SnapshotRecorder:
    def __init__(
        self,
        config: dict,
        snapshot_path: Path,
        settlement_path: Path,
        snapshot_interval: float = 1.0,
        min_price_move_pct: float = 0.0005,
    ):
        self.config = config
        strat = config.get("strategy", {})
        self.snapshot_path = snapshot_path
        self.settlement_path = settlement_path
        self.snapshot_interval = snapshot_interval
        self.min_price_move_pct = min_price_move_pct

        assets = [a.replace("usdt", "") for a in strat.get("symbols", ["btcusdt", "ethusdt", "solusdt"])]
        self.gamma = PolymarketGammaClient()
        self.registry = MarketRegistry(
            self.gamma,
            assets=assets,
            refresh_interval=strat.get("registry_refresh_sec", 15),
            min_liquidity=strat.get("min_liquidity", 1000),
        )
        self.stream = BinanceStream(
            symbols=strat.get("symbols", ["btcusdt", "ethusdt", "solusdt"]),
            on_tick=self.on_tick,
        )

        self._snapshot_fp = None
        self._settlement_fp = None
        self._last_snapshot_ts: dict[str, float] = {}
        self._last_snapshot_price: dict[str, float] = {}
        self._last_snapshot_window: dict[str, int] = {}
        self._window_state: dict[tuple[str, int], dict] = {}
        self._running = False

    async def start(self, duration_secs: float = 0.0):
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self.settlement_path.parent.mkdir(parents=True, exist_ok=True)
        self._snapshot_fp = self.snapshot_path.open("a", buffering=1)
        self._settlement_fp = self.settlement_path.open("a", buffering=1)

        await self.registry.refresh()
        registry_task = asyncio.create_task(self.registry.run())
        stream_task = asyncio.create_task(self.stream.run())
        self._running = True

        logger.info(f"Recording snapshots -> {self.snapshot_path}")
        logger.info(f"Recording settlements -> {self.settlement_path}")

        try:
            if duration_secs > 0:
                await asyncio.sleep(duration_secs)
            else:
                while self._running:
                    await asyncio.sleep(1.0)
        finally:
            self.stream.stop()
            self.registry.stop()
            registry_task.cancel()
            stream_task.cancel()
            self._flush_open_windows()
            await self.gamma.close()
            if self._snapshot_fp is not None:
                self._snapshot_fp.close()
            if self._settlement_fp is not None:
                self._settlement_fp.close()

    def stop(self):
        self._running = False

    async def on_tick(self, tick: Tick):
        self.registry.buffer_tick(tick.symbol, tick.price, tick.timestamp)
        self.registry.record_opening_price(tick.symbol, tick.price, tick.timestamp)

        market = self.registry.get_market(tick.symbol)
        if market is None:
            return

        window_key = (tick.symbol, market.event_start)
        current = self._window_state.get(window_key)
        if current is None:
            self._finalize_previous_window(tick.symbol, market.event_start)
            current = {
                "symbol": tick.symbol,
                "asset": market.asset.upper(),
                "window_start": market.event_start,
                "opening_price": market.opening_price,
                "last_price": tick.price,
                "last_tick_ts": tick.timestamp,
                "end_time": market.end_time,
            }
            self._window_state[window_key] = current
        else:
            current["last_price"] = tick.price
            current["last_tick_ts"] = tick.timestamp
            if market.has_opening_price:
                current["opening_price"] = market.opening_price

        if not self._should_snapshot(tick, market):
            return

        row = self._build_snapshot_row(tick, market)
        self._write_jsonl(self._snapshot_fp, row)
        self._last_snapshot_ts[tick.symbol] = tick.timestamp
        self._last_snapshot_price[tick.symbol] = tick.price
        self._last_snapshot_window[tick.symbol] = market.event_start

    def _should_snapshot(self, tick: Tick, market) -> bool:
        prev_ts = self._last_snapshot_ts.get(tick.symbol, 0.0)
        prev_price = self._last_snapshot_price.get(tick.symbol, 0.0)
        prev_window = self._last_snapshot_window.get(tick.symbol, -1)

        if prev_window != market.event_start:
            return True
        if tick.timestamp - prev_ts >= self.snapshot_interval:
            return True
        if prev_price <= 0:
            return True

        move_pct = abs(tick.price - prev_price) / prev_price
        return move_pct >= self.min_price_move_pct

    def _build_snapshot_row(self, tick: Tick, market) -> dict:
        deviation = 0.0
        if market.has_opening_price and market.opening_price > 0:
            deviation = (tick.price - market.opening_price) / market.opening_price

        return {
            "timestamp": tick.timestamp,
            "symbol": tick.symbol,
            "asset": market.asset.upper(),
            "binance_price": tick.price,
            "quantity": tick.quantity,
            "window_start": market.event_start,
            "event_start": market.event_start,
            "end_time": market.end_time,
            "secs_remaining": max(0.0, market.end_time - tick.timestamp),
            "secs_elapsed": max(0.0, tick.timestamp - market.event_start),
            "opening_price": market.opening_price,
            "has_opening_price": market.has_opening_price,
            "deviation_pct": deviation,
            "market_id": market.market_id,
            "market_slug": market.slug,
            "question": market.question,
            "up_token_id": market.up_token_id,
            "down_token_id": market.down_token_id,
            "up_price": market.up_price,
            "down_price": market.down_price,
            "best_bid": market.best_bid,
            "best_ask": market.best_ask,
            "spread": market.spread,
            "volume": market.volume,
            "liquidity": market.liquidity,
            "fee_rate": market.fee_rate,
            "fees_enabled": market.fees_enabled,
            "order_min_size": market.order_min_size,
        }

    def _finalize_previous_window(self, symbol: str, next_window_start: int):
        obsolete = [
            key for key in self._window_state
            if key[0] == symbol and key[1] < next_window_start
        ]
        for key in obsolete:
            state = self._window_state.pop(key)
            self._emit_settlement(state)

    def _flush_open_windows(self):
        for state in list(self._window_state.values()):
            self._emit_settlement(state)
        self._window_state.clear()

    def _emit_settlement(self, state: dict):
        opening_price = float(state.get("opening_price", 0) or 0)
        final_price = float(state.get("last_price", 0) or 0)
        settle_side = ""
        if opening_price > 0 and final_price > 0:
            settle_side = "UP" if final_price >= opening_price else "DOWN"

        row = {
            "symbol": state["symbol"],
            "asset": state["asset"],
            "window_start": state["window_start"],
            "opening_price": opening_price,
            "final_price": final_price,
            "settle_side": settle_side,
            "last_tick_ts": state.get("last_tick_ts", 0),
            "end_time": state.get("end_time", 0),
            "recorded_at": time.time(),
        }
        self._write_jsonl(self._settlement_fp, row)

    @staticmethod
    def _write_jsonl(fp, row: dict):
        if fp is None:
            return
        fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def default_paths() -> tuple[Path, Path]:
    ts = time.strftime("%Y%m%d_%H%M%S")
    base = Path("data") / "recordings"
    return (
        base / f"latency_snapshots_{ts}.jsonl",
        base / f"latency_settlements_{ts}.jsonl",
    )


async def async_main(args):
    with Path(args.config).open() as f:
        config = yaml.safe_load(f)

    snapshot_path = Path(args.snapshot_path) if args.snapshot_path else default_paths()[0]
    settlement_path = Path(args.settlement_path) if args.settlement_path else default_paths()[1]

    recorder = SnapshotRecorder(
        config=config,
        snapshot_path=snapshot_path,
        settlement_path=settlement_path,
        snapshot_interval=args.snapshot_interval,
        min_price_move_pct=args.min_price_move_pct,
    )

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            loop.add_signal_handler(getattr(signal, sig_name), recorder.stop)

    await recorder.start(duration_secs=args.duration_secs)


def main():
    parser = argparse.ArgumentParser(description="Record Binance/Polymarket snapshots for replay.")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--snapshot-path", default="")
    parser.add_argument("--settlement-path", default="")
    parser.add_argument("--snapshot-interval", type=float, default=1.0)
    parser.add_argument("--min-price-move-pct", type=float, default=0.0005)
    parser.add_argument("--duration-secs", type=float, default=0.0, help="0 means run until stopped")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
