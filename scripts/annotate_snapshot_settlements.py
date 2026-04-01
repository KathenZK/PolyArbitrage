from __future__ import annotations

import argparse
import json
from pathlib import Path


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


def annotate(snapshots_path: Path, settlements_path: Path, output_path: Path):
    settlements = load_settlements(settlements_path)
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
                row["final_price"] = settlement.get("final_price", "")
                row["settle_side"] = settlement.get("settle_side", "")
            dst.write(json.dumps(row, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Join snapshot rows with recorded settlement rows.")
    parser.add_argument("snapshots")
    parser.add_argument("settlements")
    parser.add_argument("output")
    args = parser.parse_args()

    annotate(Path(args.snapshots), Path(args.settlements), Path(args.output))


if __name__ == "__main__":
    main()
