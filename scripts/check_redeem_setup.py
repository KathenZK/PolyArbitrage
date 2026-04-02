from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data.polymarket_client import PolymarketGammaClient
from src.execution.redeemer import ProxyRedeemer


def load_config() -> dict:
    with open(ROOT / "config.yaml") as f:
        return yaml.safe_load(f)


async def _run(verbose: bool) -> int:
    load_dotenv(ROOT / ".env")
    config = load_config()
    redeem_cfg = config.get("redeem", {})

    gamma = PolymarketGammaClient()
    worker = ProxyRedeemer(
        gamma,
        enabled=redeem_cfg.get("enabled", True),
        poll_interval_secs=redeem_cfg.get("poll_interval_sec", 180),
        tracked_strategy_only=redeem_cfg.get("tracked_strategy_only", True),
        require_auth=redeem_cfg.get("require_auth", True),
    )

    try:
        report = await worker.preflight()
    finally:
        await gamma.close()

    print("Redeem setup check")
    print(f"  OK:      {'yes' if report.ok else 'no'}")
    print(f"  Enabled: {'yes' if report.enabled else 'no'}")
    print(f"  Owner:   {report.owner or '-'}")
    print(f"  Funder:  {report.funder or '-'}")
    print(f"  Proxy:   {report.derived_proxy or '-'}")
    print(f"  Relay:   {report.relay_address or '-'}")
    print(f"  Nonce:   {report.relay_nonce or '-'}")
    print(f"  KeyAddr: {report.api_key_address or '-'}")

    if report.warnings and verbose:
        for warning in report.warnings:
            print(f"  Warn:    {warning}")

    if report.issues:
        for issue in report.issues:
            print(f"  Issue:   {issue}")
        return 1

    return 0


def main():
    parser = argparse.ArgumentParser(description="Check relayer/redeem setup without submitting transactions.")
    parser.add_argument("--quiet", action="store_true", help="Suppress warning lines on success.")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(verbose=not args.quiet)))


if __name__ == "__main__":
    main()
