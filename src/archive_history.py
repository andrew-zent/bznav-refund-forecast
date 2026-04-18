"""주간 raw deal 스냅샷을 SQLite에 append.

output/history.sqlite (primary key: as_of_date, deal_id, source)
— 매주 전체 deal 상태를 append → 과거 임의 시점의 pipeline/status 재현 가능.

Workflow에서 model.py 이후 호출 → release에 업로드.
"""
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
DB_PATH = OUTPUT_DIR / "history.sqlite"

SCHEMA = """
CREATE TABLE IF NOT EXISTS deal_history (
  as_of_date        TEXT NOT NULL,
  deal_id           INTEGER NOT NULL,
  source            TEXT NOT NULL,
  status            TEXT,
  pipeline          TEXT,
  apply_date        TEXT,
  apply_amount      REAL,
  filing_date       TEXT,
  filing_amount     REAL,
  decision_date     TEXT,
  decision_amount   REAL,
  payment_date      TEXT,
  payment_amount    REAL,
  lost_reason       TEXT,
  lost_time         TEXT,
  cancel_reason     TEXT,
  cancel_reason_auto TEXT,
  hold_reason       TEXT,
  hold_reason_2     TEXT,
  customer_type     TEXT,
  channel           TEXT,
  channel_id        TEXT,
  utm_source        TEXT,
  utm_medium        TEXT,
  utm_campaign      TEXT,
  utm_source_query  TEXT,
  utm_medium_query  TEXT,
  utm_campaign_query TEXT,
  is_only_gam       TEXT,
  update_time       TEXT,
  PRIMARY KEY (as_of_date, deal_id, source)
);

CREATE INDEX IF NOT EXISTS idx_deal_id    ON deal_history(deal_id);
CREATE INDEX IF NOT EXISTS idx_date       ON deal_history(as_of_date);
CREATE INDEX IF NOT EXISTS idx_pipeline   ON deal_history(pipeline);
CREATE INDEX IF NOT EXISTS idx_status     ON deal_history(status);
CREATE INDEX IF NOT EXISTS idx_apply_date ON deal_history(apply_date);

CREATE TABLE IF NOT EXISTS archive_runs (
  as_of_date   TEXT PRIMARY KEY,
  generated_at TEXT,
  n_deals      INTEGER,
  n_indiv      INTEGER,
  n_corp       INTEGER
);
"""

COLS = [
    "as_of_date", "deal_id", "source", "status", "pipeline",
    "apply_date", "apply_amount", "filing_date", "filing_amount",
    "decision_date", "decision_amount", "payment_date", "payment_amount",
    "lost_reason", "lost_time", "cancel_reason", "cancel_reason_auto",
    "hold_reason", "hold_reason_2", "customer_type",
    "channel", "channel_id", "utm_source", "utm_medium", "utm_campaign",
    "utm_source_query", "utm_medium_query", "utm_campaign_query",
    "is_only_gam", "update_time",
]


def _row(d, as_of_date, source):
    return tuple([
        as_of_date,
        d.get("id"),
        source,
        d.get("status"),
        d.get("pipeline"),
        d.get("apply_date"),
        d.get("apply_amount"),
        d.get("filing_date"),
        d.get("filing_amount"),
        d.get("decision_date"),
        d.get("decision_amount"),
        d.get("payment_date"),
        d.get("payment_amount"),
        d.get("lost_reason"),
        d.get("lost_time"),
        d.get("cancel_reason"),
        d.get("cancel_reason_auto"),
        d.get("hold_reason"),
        d.get("hold_reason_2"),
        d.get("customer_type"),
        d.get("channel"),
        d.get("channel_id"),
        d.get("utm_source"),
        d.get("utm_medium"),
        d.get("utm_campaign"),
        d.get("utm_source_query"),
        d.get("utm_medium_query"),
        d.get("utm_campaign_query"),
        d.get("is_only_gam"),
        d.get("update_time"),
    ])


def main():
    indiv_path = DATA_DIR / "deals_slim.json"
    corp_path = DATA_DIR / "deals_corp_slim.json"
    if not indiv_path.exists() and not corp_path.exists():
        print("ERROR: no slim data in data/. Run extract_*.py first.", file=sys.stderr)
        sys.exit(1)

    as_of_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)

    total = n_indiv = n_corp = 0

    # 같은 날짜 중복 방지: 먼저 삭제
    conn.execute("DELETE FROM deal_history WHERE as_of_date = ?", (as_of_date,))

    placeholders = ",".join(["?"] * len(COLS))
    insert_sql = f"INSERT INTO deal_history ({','.join(COLS)}) VALUES ({placeholders})"

    if indiv_path.exists():
        deals = json.loads(indiv_path.read_text())
        conn.executemany(insert_sql, (_row(d, as_of_date, "indiv") for d in deals))
        n_indiv = len(deals)
        total += n_indiv
        print(f"  indiv: {n_indiv:,} deals")

    if corp_path.exists():
        deals = json.loads(corp_path.read_text())
        conn.executemany(insert_sql, (_row(d, as_of_date, "corp") for d in deals))
        n_corp = len(deals)
        total += n_corp
        print(f"  corp : {n_corp:,} deals")

    conn.execute(
        "INSERT OR REPLACE INTO archive_runs (as_of_date, generated_at, n_deals, n_indiv, n_corp) VALUES (?, ?, ?, ?, ?)",
        (as_of_date, generated_at, total, n_indiv, n_corp),
    )
    conn.commit()

    # 통계
    cur = conn.cursor()
    cur.execute("SELECT COUNT(DISTINCT as_of_date) FROM deal_history")
    n_weeks = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM deal_history")
    n_rows = cur.fetchone()[0]

    conn.execute("VACUUM")
    conn.close()

    size_mb = DB_PATH.stat().st_size / 1e6
    print(f"\n→ {DB_PATH}")
    print(f"   snapshots: {n_weeks} weeks, rows: {n_rows:,}, size: {size_mb:.1f} MB")


if __name__ == "__main__":
    main()
