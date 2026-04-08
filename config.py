"""
수급 동향 대시보드 — 설정
"""
import os

# ── KIS API ──────────────────────────────────────────────
KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
KIS_BASE_URL = os.environ.get(
    "KIS_BASE_URL", "https://openapi.koreainvestment.com:9443"
)

# ── DB ───────────────────────────────────────────────────
DB_PATH = os.environ.get("DB_PATH", os.path.join("storage", "supply_demand.db"))

# ── 수집 스케줄 ──────────────────────────────────────────
SUPPLY_DEMAND_ENABLED = os.environ.get("SUPPLY_DEMAND_ENABLED", "true").lower() == "true"
SUPPLY_DEMAND_COLLECT_HOUR = int(os.environ.get("SUPPLY_DEMAND_COLLECT_HOUR", "15"))
SUPPLY_DEMAND_COLLECT_MINUTE = int(os.environ.get("SUPPLY_DEMAND_COLLECT_MINUTE", "40"))

# ── 기타 ─────────────────────────────────────────────────
TIMEZONE = "Asia/Seoul"
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
