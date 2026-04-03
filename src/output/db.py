"""SQLite persistence for trades and order reconciliation state."""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "polyarbitrage.db"

TRADE_COLUMNS: dict[str, str] = {
    "updated_at": "REAL NOT NULL DEFAULT 0",
    "asset": "TEXT NOT NULL DEFAULT ''",
    "condition_id": "TEXT DEFAULT ''",
    "market_slug": "TEXT DEFAULT ''",
    "order_side": "TEXT NOT NULL DEFAULT 'BUY'",
    "order_id": "TEXT DEFAULT ''",
    "matched_size": "REAL NOT NULL DEFAULT 0",
    "matched_cost_usd": "REAL NOT NULL DEFAULT 0",
    "win_prob": "REAL NOT NULL DEFAULT 0",
    "fill_prob": "REAL NOT NULL DEFAULT 0",
    "fill_lower_bound": "REAL NOT NULL DEFAULT 0",
    "fill_confidence": "REAL NOT NULL DEFAULT 0",
    "fill_effective_samples": "REAL NOT NULL DEFAULT 0",
    "fill_source": "TEXT DEFAULT ''",
    "filled_ev_usd": "REAL NOT NULL DEFAULT 0",
    "expected_value_usd": "REAL NOT NULL DEFAULT 0",
    "taker_fee_avoided": "REAL NOT NULL DEFAULT 0",
    "expiration_ts": "INTEGER NOT NULL DEFAULT 0",
    "secs_remaining_at_submit": "REAL NOT NULL DEFAULT 0",
    "liquidity_at_submit": "REAL NOT NULL DEFAULT 0",
    "spread_at_submit": "REAL NOT NULL DEFAULT 0",
    "queue_ticks_at_submit": "REAL NOT NULL DEFAULT 0",
    "tick_size_at_submit": "REAL NOT NULL DEFAULT 0.01",
    "last_error": "TEXT DEFAULT ''",
    "raw_json": "TEXT DEFAULT '{}'",
}

REDEEM_COLUMNS: dict[str, str] = {
    "market_slug": "TEXT DEFAULT ''",
    "proxy_wallet": "TEXT DEFAULT ''",
    "outcome": "TEXT DEFAULT ''",
    "size": "REAL NOT NULL DEFAULT 0",
    "transaction_id": "TEXT DEFAULT ''",
    "transaction_hash": "TEXT DEFAULT ''",
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


def _ensure_redeem_columns(conn: sqlite3.Connection):
    existing = _column_names(conn, "redeems")
    for name, ddl in REDEEM_COLUMNS.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE redeems ADD COLUMN {name} {ddl}")


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

        CREATE TABLE IF NOT EXISTS redeems (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at          REAL NOT NULL,
            updated_at          REAL NOT NULL,
            condition_id        TEXT NOT NULL,
            asset               TEXT NOT NULL DEFAULT '',
            status              TEXT NOT NULL DEFAULT 'redeemable'
        );
        """
    )
    _ensure_trade_columns(conn)
    _ensure_redeem_columns(conn)
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
        CREATE INDEX IF NOT EXISTS idx_trades_order_id ON trades(order_id);
        CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_trades_condition_id ON trades(condition_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_redeems_condition_id ON redeems(condition_id);
        CREATE INDEX IF NOT EXISTS idx_redeems_status ON redeems(status);
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
    order_side: str,
    asset: str,
    market_id: str,
    market_slug: str,
    condition_id: str,
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
    fill_prob: float,
    fill_lower_bound: float,
    fill_confidence: float,
    fill_effective_samples: float,
    fill_source: str,
    filled_ev_usd: float,
    expected_value_usd: float,
    taker_fee_avoided: float,
    expiration_ts: int = 0,
    secs_remaining_at_submit: float = 0.0,
    liquidity_at_submit: float = 0.0,
    spread_at_submit: float = 0.0,
    queue_ticks_at_submit: float = 0.0,
    tick_size_at_submit: float = 0.01,
    last_error: str = "",
    raw_data: Any | None = None,
) -> int:
    now = time.time()
    cur = conn.execute(
        """
        INSERT INTO trades (
            timestamp, updated_at, strategy, event_title, action, side, asset,
            market_id, condition_id, market_slug, token_id, order_side, order_id, price, size,
            matched_size, cost_usd, matched_cost_usd, is_paper, status,
            win_prob, fill_prob, fill_lower_bound, fill_confidence,
            fill_effective_samples, fill_source, filled_ev_usd, expected_value_usd,
            taker_fee_avoided, expiration_ts, secs_remaining_at_submit,
            liquidity_at_submit, spread_at_submit, queue_ticks_at_submit,
            tick_size_at_submit,
            last_error, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            condition_id,
            market_slug,
            token_id,
            order_side,
            order_id,
            price,
            size,
            matched_size,
            cost_usd,
            matched_cost_usd,
            1 if is_paper else 0,
            status,
            win_prob,
            fill_prob,
            fill_lower_bound,
            fill_confidence,
            fill_effective_samples,
            fill_source,
            filled_ev_usd,
            expected_value_usd,
            taker_fee_avoided,
            expiration_ts,
            secs_remaining_at_submit,
            liquidity_at_submit,
            spread_at_submit,
            queue_ticks_at_submit,
            tick_size_at_submit,
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


def get_tracked_live_condition_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT condition_id
        FROM trades
        WHERE is_paper=0
          AND condition_id <> ''
        """
    ).fetchall()
    return {str(row["condition_id"]) for row in rows if row["condition_id"]}


def get_position_rows(conn: sqlite3.Connection, *, is_paper: bool) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            asset,
            market_id,
            condition_id,
            market_slug,
            token_id,
            side AS token_side,
            action AS direction,
            SUM(
                CASE
                    WHEN COALESCE(order_side, 'BUY')='BUY' THEN matched_size
                    ELSE -matched_size
                END
            ) AS net_shares,
            SUM(
                CASE
                    WHEN COALESCE(order_side, 'BUY')='SELL' AND status='pending' AND size > matched_size
                    THEN size - matched_size
                    ELSE 0
                END
            ) AS pending_sell_shares,
            SUM(
                CASE
                    WHEN COALESCE(order_side, 'BUY')='BUY' THEN matched_cost_usd
                    ELSE 0
                END
            ) AS gross_buy_cost_usd,
            SUM(
                CASE
                    WHEN COALESCE(order_side, 'BUY')='BUY' THEN matched_size
                    ELSE 0
                END
            ) AS gross_buy_shares,
            MAX(timestamp) AS last_trade_ts
        FROM trades
        WHERE is_paper=?
          AND token_id <> ''
        GROUP BY asset, market_id, condition_id, market_slug, token_id, side, action
        HAVING ABS(net_shares) > 1e-9
        ORDER BY last_trade_ts ASC
        """,
        (1 if is_paper else 0,),
    ).fetchall()
    return [dict(row) for row in rows]


def upsert_redeem_candidate(
    conn: sqlite3.Connection,
    *,
    condition_id: str,
    asset: str,
    market_slug: str,
    proxy_wallet: str,
    outcome: str,
    size: float,
    status: str = "redeemable",
    raw_data: Any | None = None,
) -> int:
    existing = conn.execute(
        "SELECT id, status FROM redeems WHERE condition_id=?",
        (condition_id,),
    ).fetchone()
    now = time.time()
    if existing is None:
        cur = conn.execute(
            """
            INSERT INTO redeems (
                created_at, updated_at, condition_id, asset, market_slug,
                proxy_wallet, outcome, size, status, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                now,
                condition_id,
                asset,
                market_slug,
                proxy_wallet,
                outcome,
                size,
                status,
                json.dumps(raw_data or {}),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)

    redeem_id = int(existing["id"])
    fields = [
        "updated_at=?",
        "asset=?",
        "market_slug=?",
        "proxy_wallet=?",
        "outcome=?",
        "size=?",
        "raw_json=?",
    ]
    values: list[Any] = [
        now,
        asset,
        market_slug,
        proxy_wallet,
        outcome,
        size,
        json.dumps(raw_data or {}),
    ]
    current_status = str(existing["status"] or "")
    if current_status not in {"submitted", "confirmed"}:
        fields.append("status=?")
        values.append(status)
    values.append(redeem_id)
    conn.execute(f"UPDATE redeems SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()
    return redeem_id


def get_pending_redeems(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM redeems
        WHERE status IN ('redeemable', 'retry', 'submitted')
        ORDER BY created_at ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def update_redeem(
    conn: sqlite3.Connection,
    redeem_id: int,
    *,
    status: str | None = None,
    transaction_id: str | None = None,
    transaction_hash: str | None = None,
    last_error: str | None = None,
    raw_data: Any | None = None,
):
    fields = ["updated_at=?"]
    values: list[Any] = [time.time()]

    if status is not None:
        fields.append("status=?")
        values.append(status)
    if transaction_id is not None:
        fields.append("transaction_id=?")
        values.append(transaction_id)
    if transaction_hash is not None:
        fields.append("transaction_hash=?")
        values.append(transaction_hash)
    if last_error is not None:
        fields.append("last_error=?")
        values.append(last_error)
    if raw_data is not None:
        fields.append("raw_json=?")
        values.append(json.dumps(raw_data))

    values.append(redeem_id)
    conn.execute(f"UPDATE redeems SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()


def get_fill_rate_stats(
    conn: sqlite3.Connection,
    *,
    asset: str | None = None,
    lookback_hours: float = 168.0,
) -> dict[str, Any]:
    since_ts = time.time() - lookback_hours * 3600
    clauses = [
        "is_paper=0",
        "status IN ('filled', 'expired', 'rejected')",
        "timestamp >= ?",
    ]
    params: list[Any] = [since_ts]
    if asset:
        clauses.append("UPPER(asset)=UPPER(?)")
        params.append(asset)

    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS samples,
            AVG(CASE WHEN size > 0 THEN matched_size / size ELSE 0 END) AS avg_fill_ratio,
            AVG(CASE WHEN status='filled' THEN 1.0 ELSE 0.0 END) AS full_fill_rate
        FROM trades
        WHERE {' AND '.join(clauses)}
        """,
        params,
    ).fetchone()
    if not row:
        return {"samples": 0, "avg_fill_ratio": 0.0, "full_fill_rate": 0.0}
    return {
        "samples": int(row["samples"] or 0),
        "avg_fill_ratio": float(row["avg_fill_ratio"] or 0.0),
        "full_fill_rate": float(row["full_fill_rate"] or 0.0),
    }


def get_fill_calibration_rows(
    conn: sqlite3.Connection,
    *,
    lookback_hours: float = 168.0,
) -> list[dict[str, Any]]:
    since_ts = time.time() - lookback_hours * 3600
    rows = conn.execute(
        """
        SELECT
            timestamp,
            asset,
            size,
            matched_size,
            status,
            secs_remaining_at_submit,
            liquidity_at_submit,
            spread_at_submit,
            queue_ticks_at_submit
        FROM trades
        WHERE is_paper=0
          AND status IN ('filled', 'expired', 'rejected')
          AND timestamp >= ?
          AND size > 0
        ORDER BY timestamp DESC
        """,
        (since_ts,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_live_daily_usage(conn: sqlite3.Connection, *, now_ts: float | None = None) -> dict[str, float]:
    now = datetime.fromtimestamp(now_ts or time.time()).astimezone()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS orders,
            COALESCE(SUM(cost_usd), 0) AS submitted_notional,
            COALESCE(SUM(matched_cost_usd), 0) AS matched_notional
        FROM trades
        WHERE is_paper=0
          AND timestamp >= ?
        """,
        (start_of_day,),
    ).fetchone()
    if row is None:
        return {"orders": 0.0, "submitted_notional": 0.0, "matched_notional": 0.0}
    return {
        "orders": float(row["orders"] or 0),
        "submitted_notional": float(row["submitted_notional"] or 0),
        "matched_notional": float(row["matched_notional"] or 0),
    }
