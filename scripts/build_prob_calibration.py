from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

from src.strategies.momentum import ProbabilityCalibrator
from src.strategies.replay import _settle_side, load_replay_rows, signal_from_row


def build_calibration(
    paths: list[Path],
    *,
    threshold_pct: float,
    min_secs_remaining: float,
    min_secs_elapsed: float,
    require_official_source: bool,
    official_max_age_secs: float,
    max_source_divergence_pct: float,
    source_gap_penalty_mult: float,
) -> dict[str, dict[str, float]]:
    buckets: dict[str, dict[str, float]] = defaultdict(lambda: {"samples": 0.0, "wins": 0.0})

    for path in paths:
        rows = load_replay_rows(path)
        for row in rows:
            signal = signal_from_row(
                row,
                threshold_pct=threshold_pct,
                min_secs_remaining=min_secs_remaining,
                min_secs_elapsed=min_secs_elapsed,
                require_official_source=require_official_source,
                official_max_age_secs=official_max_age_secs,
                max_source_divergence_pct=max_source_divergence_pct,
                source_gap_penalty_mult=source_gap_penalty_mult,
            )
            if signal is None:
                continue
            settle_side = _settle_side(row, signal.opening_price)
            if settle_side not in {"UP", "DOWN"}:
                continue

            key = ProbabilityCalibrator.bucket_key(
                asset=signal.asset,
                deviation_abs=abs(signal.deviation_pct),
                secs_remaining=signal.market.secs_remaining,
                source_gap=signal.source_gap_pct,
            )
            bucket = buckets[key]
            bucket["samples"] += 1.0
            if settle_side == signal.direction.value:
                bucket["wins"] += 1.0

    return {
        key: {
            "samples": value["samples"],
            "win_rate": (value["wins"] / value["samples"]) if value["samples"] > 0 else 0.5,
        }
        for key, value in sorted(buckets.items())
    }


def main():
    parser = argparse.ArgumentParser(description="Build empirical win-prob calibration from annotated replay rows.")
    parser.add_argument("inputs", nargs="+", help="Annotated JSONL/CSV replay inputs")
    parser.add_argument("--output", required=True, help="Output JSON path")
    parser.add_argument("--threshold-pct", type=float, default=0.003)
    parser.add_argument("--min-secs-remaining", type=float, default=30.0)
    parser.add_argument("--min-secs-elapsed", type=float, default=30.0)
    parser.add_argument("--require-official-source", action="store_true")
    parser.add_argument("--official-max-age-sec", type=float, default=15.0)
    parser.add_argument("--max-source-divergence-pct", type=float, default=0.0025)
    parser.add_argument("--source-gap-penalty-mult", type=float, default=8.0)
    args = parser.parse_args()

    input_paths = [Path(value) for value in args.inputs]
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    buckets = build_calibration(
        input_paths,
        threshold_pct=args.threshold_pct,
        min_secs_remaining=args.min_secs_remaining,
        min_secs_elapsed=args.min_secs_elapsed,
        require_official_source=args.require_official_source,
        official_max_age_secs=args.official_max_age_sec,
        max_source_divergence_pct=args.max_source_divergence_pct,
        source_gap_penalty_mult=args.source_gap_penalty_mult,
    )

    payload = {
        "version": 1,
        "bucket_schema": {
            "asset": "upper-case asset",
            "secs_remaining": ["<60", "60-180", "180-420", ">=420"],
            "deviation_abs": ["<0.0030", "<0.0050", "<0.0075", "<0.0100", "<0.0150", "<0.0200", ">=0.0200"],
            "source_gap": ["<0.001", "0.001-0.0025", "0.0025-0.005", ">=0.005"],
        },
        "buckets": buckets,
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    print(f"Wrote {len(buckets)} calibration buckets to {output_path}")


if __name__ == "__main__":
    main()
