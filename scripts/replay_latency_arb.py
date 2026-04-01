from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.strategies.replay import load_replay_rows, run_replay


def main():
    parser = argparse.ArgumentParser(description="Replay the latency-arb strategy on historical snapshots.")
    parser.add_argument("input", help="CSV or JSONL with market/tick snapshots")
    parser.add_argument("--config", default="config.yaml", help="Strategy config path")
    args = parser.parse_args()

    with Path(args.config).open() as f:
        config = yaml.safe_load(f)

    rows = load_replay_rows(args.input)
    summary = run_replay(rows, config)
    print(json.dumps(summary.to_dict(), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
