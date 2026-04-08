"""
DB — SQLite 스키마 + 연결 관리
"""
import os
import sqlite3
import logging

from config import DB_PATH

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS supply_demand_daily (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at    TEXT NOT NULL,
    biz_date        TEXT NOT NULL,
    market          TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    name            TEXT NOT NULL,
    sector          TEXT,
    sector_group    TEXT,
    price           INTEGER,
    price_change_pct REAL,
    frgn_net_qty    INTEGER DEFAULT 0,
    orgn_net_qty    INTEGER DEFAULT 0,
    frgn_net_amt    INTEGER DEFAULT 0,
    orgn_net_amt    INTEGER DEFAULT 0,
    acml_vol        INTEGER DEFAULT 0,
    UNIQUE(biz_date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_sd_date ON supply_demand_daily(biz_date);
CREATE INDEX IF NOT EXISTS idx_sd_ticker ON supply_demand_daily(ticker);
CREATE INDEX IF NOT EXISTS idx_sd_sector ON supply_demand_daily(sector_group);
"""


def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
        logger.info("DB 초기화 완료: %s", DB_PATH)
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
