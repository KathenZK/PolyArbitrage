"""SQLite persistence for trades and order reconciliation state."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "polyarbitrage.db"

TRADE_COLUMNS: dict[str, str] = {
    "updated_at": "REAL NOT NULL DEFAULT 0",
    "asset": "TEXT NOT NULL DEFAULT ''",
    "market_slug": "TEXT DEFAULT ''",
    "order_id": "TEXT DEFAULT ''",
    "matched_size": "REAL NOT NULL DEFAULT 0",
    "matched_cost_usd": "REAL NOT NULL DEFAULT 0",
    "win_prob": "REAL NOT NULL DEFAULT 0",
    "expected_value_usd": "REAL NOT NULL DEFAULT 0",
    "taker_fee_avoided": "REAL NOT NULL DEFAULT 0",
    "expiration_ts": "INTEGER NOT NULL DEFAULT 0",
    "last_error": "TEXT DEFAULT ''",
    "raw_json": "TEXT DEFAULT '{}'",
}


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row["name"]) for row in rows}


def _ensure_trade_columns(conn: sqlite3.Connection):
    existing = _column_names(conn, "trades")
    for name, ddl in TRADE_COLUMNS.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE trades ADD COLUMN {name} {ddl}")


def init_db(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp           REAL NOT NULL,
            strategy            TEXT NOT NULL,
            event_title         TEXT NOT NULL,
            action              TEXT NOT NULL,
            side                TEXT NOT NULL,
            market_id           TEXT NOT NULL,
            token_id            TEXT,
            price               REAL NOT NULL,
            size                REAL NOT NULL,
            cost_usd            REAL NOT NULL,
            is_paper            INTEGER DEFAULT 1,
            status              TEXT DEFAULT 'pending',
            pnl                 REAL DEFAULT 0.0,
            resolved_at         REAL
        );
        """
    )
    _ensure_trade_columns(conn)
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
        CREATE INDEX IF NOT EXISTS idx_trades_order_id ON trades(order_id);
        CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
        """
    )
    conn.execute(
        "UPDATE trades SET status='pending' WHERE status IN ('open', 'live', 'unmatched')"
    )
    conn.execute(
        "UPDATE trades SET updated_at=timestamp WHERE updated_at=0"
    )
    conn.commit()


def insert_trade(
    conn: sqlite3.Connection,
    *,
    strategy: str,
    event_title: str,
    action: str,
    side: str,
    asset: str,
    market_id: str,
    market_slug: str,
    token_id: str,
    price: float,
    size: float,
    matched_size: float,
    cost_usd: float,
    matched_cost_usd: float,
    is_paper: bool,
    status: str,
    order_id: str,
    win_prob: float,
    expected_value_usd: float,
    taker_fee_avoided: float,
    expiration_ts: int = 0,
    last_error: str = "",
    raw_data: Any | None = None,
) -> int:
    now = time.time()
    cur = conn.execute(
        """
        INSERT INTO trades (
            timestamp, updated_at, strategy, event_title, action, side, asset,
            market_id, market_slug, token_id, order_id, price, size,
            matched_size, cost_usd, matched_cost_usd, is_paper, status,
            win_prob, expected_value_usd, taker_fee_avoided, expiration_ts,
            last_error, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            now,
            now,
            strategy,
            event_title,
            action,
            side,
            asset,
            market_id,
            market_slug,
            token_id,
            order_id,
            price,
            size,
            matched_size,
            cost_usd,
            matched_cost_usd,
            1 if is_paper else 0,
            status,
            win_prob,
            expected_value_usd,
            taker_fee_avoided,
            expiration_ts,
            last_error,
            json.dumps(raw_data or {}),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def update_trade(
    conn: sqlite3.Connection,
    trade_id: int,
    *,
    status: str | None = None,
    matched_size: float | None = None,
    matched_cost_usd: float | None = None,
    order_id: str | None = None,
    last_error: str | None = None,
    raw_data: Any | None = None,
):
    fields: list[str] = ["updated_at=?"]
    values: list[Any] = [time.time()]

    if status is not None:
        fields.append("status=?")
        values.append(status)
        if status in {"filled", "expired", "rejected"}:
            fields.append("resolved_at=?")
            values.append(time.time())
    if matched_size is not None:
        fields.append("matched_size=?")
        values.append(matched_size)
    if matched_cost_usd is not None:
        fields.append("matched_cost_usd=?")
        values.append(matched_cost_usd)
    if order_id is not None:
        fields.append("order_id=?")
        values.append(order_id)
    if last_error is not None:
        fields.append("last_error=?")
        values.append(last_error)
    if raw_data is not None:
        fields.append("raw_json=?")
        values.append(json.dumps(raw_data))

    values.append(trade_id)
    conn.execute(f"UPDATE trades SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()


def get_pending_trades(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM trades
        WHERE is_paper=0 AND status='pending' AND order_id <> ''
        ORDER BY timestamp ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]
