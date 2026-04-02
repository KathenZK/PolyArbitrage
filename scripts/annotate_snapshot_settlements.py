from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.polymarket_client import PolymarketGammaClient


def load_settlements(path: Path) -> dict[tuple[str, int], dict]:
    mapping: dict[tuple[str, int], dict] = {}
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            key = (str(row["symbol"]).lower(), int(row["window_start"]))
            mapping[key] = row
    return mapping


async def fetch_resolved_truths(slugs: set[str]) -> dict[str, dict]:
    gamma = PolymarketGammaClient()
    results: dict[str, dict] = {}
    try:
        semaphore = asyncio.Semaphore(8)

        async def fetch_one(slug: str):
            async with semaphore:
                try:
                    results[slug] = await gamma.get_resolved_truth(slug)
                except Exception as exc:
                    results[slug] = {
                        "market_slug": slug,
                        "resolved_truth_available": False,
                        "resolved_truth_source": "",
                        "resolved_error": str(exc),
                    }

        await asyncio.gather(*(fetch_one(slug) for slug in sorted(slugs)))
    finally:
        await gamma.close()
    return results


def annotate(snapshots_path: Path, settlements_path: Path, output_path: Path, resolved_truths: dict[str, dict] | None = None):
    settlements = load_settlements(settlements_path)
    resolved_truths = resolved_truths or {}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with snapshots_path.open() as src, output_path.open("w") as dst:
        for line in src:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            key = (str(row.get("symbol", "")).lower(), int(row.get("window_start", 0)))
            settlement = settlements.get(key)
            if settlement:
                row["recorded_final_price"] = settlement.get("final_price", "")
                row["recorded_settle_side"] = settlement.get("settle_side", "")
                row["recorded_official_final_price"] = settlement.get("official_final_price", "")
                row["recorded_official_settle_side"] = settlement.get("official_settle_side", "")

            slug = str(row.get("market_slug", "") or "")
            if slug and slug in resolved_truths:
                row.update(resolved_truths[slug])
            dst.write(json.dumps(row, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Join snapshot rows with recorded settlement rows.")
    parser.add_argument("snapshots")
    parser.add_argument("settlements")
    parser.add_argument("output")
    args = parser.parse_args()

    snapshots_path = Path(args.snapshots)
    slugs: set[str] = set()
    with snapshots_path.open() as f:
        for raw_line in f:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            row = json.loads(raw_line)
            slug = str(row.get("market_slug", "") or "")
            if slug:
                slugs.add(slug)

    resolved_truths = asyncio.run(fetch_resolved_truths(slugs))
    annotate(snapshots_path, Path(args.settlements), Path(args.output), resolved_truths)


if __name__ == "__main__":
    main()
