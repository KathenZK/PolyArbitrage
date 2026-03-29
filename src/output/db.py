"""SQLite persistence layer for opportunities, trades, and calibration data."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "polyarbitrage.db"


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS opportunities (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       REAL NOT NULL,
        strategy        TEXT NOT NULL,
        event_title     TEXT NOT NULL,
        action          TEXT NOT NULL,
        edge_pct        REAL NOT NULL,
        details_json    TEXT,
        market_ids_json TEXT,
        settlement_date TEXT,
        status          TEXT DEFAULT 'detected'
    );

    CREATE TABLE IF NOT EXISTS trades (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       REAL NOT NULL,
        strategy        TEXT NOT NULL,
        event_title     TEXT NOT NULL,
        action          TEXT NOT NULL,
        side            TEXT NOT NULL,
        market_id       TEXT NOT NULL,
        token_id        TEXT,
        price           REAL NOT NULL,
        size            REAL NOT NULL,
        cost_usd        REAL NOT NULL,
        is_paper        INTEGER DEFAULT 1,
        status          TEXT DEFAULT 'open',
        pnl             REAL DEFAULT 0.0,
        resolved_at     REAL
    );

    CREATE TABLE IF NOT EXISTS calibration (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       REAL NOT NULL,
        model_name      TEXT NOT NULL,
        market_type     TEXT,
        brier_score     REAL,
        log_loss        REAL,
        accuracy        REAL,
        sample_count    INTEGER,
        weights_json    TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_opp_strategy ON opportunities(strategy);
    CREATE INDEX IF NOT EXISTS idx_opp_timestamp ON opportunities(timestamp);
    CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
    CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy);
    """)
    conn.commit()


def insert_opportunity(conn: sqlite3.Connection, strategy: str, event_title: str,
                       action: str, edge_pct: float, details: dict | None = None,
                       market_ids: list[str] | None = None, settlement_date: str = ""):
    conn.execute(
        """INSERT INTO opportunities (timestamp, strategy, event_title, action, edge_pct,
           details_json, market_ids_json, settlement_date)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (time.time(), strategy, event_title, action, edge_pct,
         json.dumps(details or {}), json.dumps(market_ids or []), settlement_date),
    )
    conn.commit()


def insert_trade(conn: sqlite3.Connection, strategy: str, event_title: str,
                 action: str, side: str, market_id: str, token_id: str,
                 price: float, size: float, cost_usd: float, is_paper: bool = True):
    conn.execute(
        """INSERT INTO trades (timestamp, strategy, event_title, action, side,
           market_id, token_id, price, size, cost_usd, is_paper)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (time.time(), strategy, event_title, action, side,
         market_id, token_id, price, size, cost_usd, 1 if is_paper else 0),
    )
    conn.commit()


def resolve_trade(conn: sqlite3.Connection, trade_id: int, pnl: float):
    conn.execute(
        "UPDATE trades SET status='resolved', pnl=?, resolved_at=? WHERE id=?",
        (pnl, time.time(), trade_id),
    )
    conn.commit()


def get_open_trades(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM trades WHERE status='open' ORDER BY timestamp DESC").fetchall()
    return [dict(r) for r in rows]


def get_recent_opportunities(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM opportunities ORDER BY timestamp DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_trade_summary(conn: sqlite3.Connection) -> dict:
    """Aggregate P&L stats across resolved trades."""
    row = conn.execute("""
        SELECT
            COUNT(*) as total_trades,
            SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
            SUM(pnl) as total_pnl,
            AVG(pnl) as avg_pnl
        FROM trades WHERE status='resolved'
    """).fetchone()
    return dict(row) if row else {}


def insert_calibration(conn: sqlite3.Connection, model_name: str, market_type: str,
                       brier_score: float, log_loss: float, accuracy: float,
                       sample_count: int, weights: dict):
    conn.execute(
        """INSERT INTO calibration (timestamp, model_name, market_type, brier_score,
           log_loss, accuracy, sample_count, weights_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (time.time(), model_name, market_type, brier_score, log_loss,
         accuracy, sample_count, json.dumps(weights)),
    )
    conn.commit()
