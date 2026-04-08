"""
Flask 앱 — 수급 동향 대시보드
"""
import logging
import os
from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template, request

from config import LOG_LEVEL, SUPPLY_DEMAND_ENABLED
from db import get_connection, init_db
from sector_loader import load_sector_data
from patterns import (
    detect_consecutive_buying,
    detect_sector_rotation,
    detect_flow_reversal,
    detect_investor_alignment,
)
from collector import collect_daily_supply_demand

# ── 로거 ─────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── 앱 초기화 ────────────────────────────────
app = Flask(__name__)
init_db()
load_sector_data()

# 스케줄러 등록
_scheduler = None
if SUPPLY_DEMAND_ENABLED:
    try:
        from scheduler import start_scheduler
        _scheduler = start_scheduler()
    except Exception as e:
        logger.error("스케줄러 시작 실패: %s", e)

# 수동 수집 1일 1회 제한 (메모리)
_last_manual_collect: datetime | None = None


# ────────────────────────────────────────────────
# 라우트
# ────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("supply_demand.html")


@app.route("/api/health")
def api_health():
    conn = get_connection()
    try:
        total = conn.execute(
            "SELECT COUNT(*) as c FROM supply_demand_daily"
        ).fetchone()["c"]
        days = conn.execute(
            "SELECT COUNT(DISTINCT biz_date) as c FROM supply_demand_daily"
        ).fetchone()["c"]
        last = conn.execute(
            "SELECT MAX(biz_date) as d, MAX(collected_at) as c FROM supply_demand_daily"
        ).fetchone()
    finally:
        conn.close()
    return jsonify({
        "status": "ok",
        "total_records": total,
        "collected_days": days,
        "last_biz_date": last["d"],
        "last_collected_at": last["c"],
    })


@app.route("/api/supply-demand")
def api_supply_demand():
    """수급 동향 데이터 통합 조회."""
    days = request.args.get("days", 20, type=int)
    market = request.args.get("market", "all")
    top_n = request.args.get("top", 30, type=int)

    conn = get_connection()
    try:
        cutoff = (datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y%m%d")

        # ── 1. 종목별 누적 순매수 ──────────────
        q_stock = """
            SELECT ticker, name, sector_group, market,
                   SUM(frgn_net_amt) as total_frgn_amt,
                   SUM(orgn_net_amt) as total_orgn_amt,
                   SUM(frgn_net_amt + orgn_net_amt) as total_net_amt,
                   COUNT(*) as appear_days,
                   AVG(price_change_pct) as avg_change_pct
            FROM supply_demand_daily
            WHERE biz_date >= ?
        """
        params: list = [cutoff]
        if market != "all":
            q_stock += " AND market = ?"
            params.append(market)
        q_stock += " GROUP BY ticker ORDER BY total_net_amt DESC"

        stock_rows = [dict(r) for r in conn.execute(q_stock, params).fetchall()]

        # 억원 변환
        def _to_eok(rec):
            rec["total_frgn_amt"] = round((rec["total_frgn_amt"] or 0) / 100, 1)
            rec["total_orgn_amt"] = round((rec["total_orgn_amt"] or 0) / 100, 1)
            rec["total_net_amt"] = round((rec["total_net_amt"] or 0) / 100, 1)
            rec["avg_change_pct"] = round(rec["avg_change_pct"] or 0, 2)
            return rec
        stock_rows = [_to_eok(r) for r in stock_rows]

        top_buy = [r for r in stock_rows if r["total_net_amt"] > 0][:top_n]
        top_sell = sorted(
            [r for r in stock_rows if r["total_net_amt"] < 0],
            key=lambda x: x["total_net_amt"],
        )[:top_n]

        # 외인/기관 각각 TOP
        frgn_top = sorted(
            [r for r in stock_rows if r["total_frgn_amt"] > 0],
            key=lambda x: -x["total_frgn_amt"],
        )[:10]
        orgn_top = sorted(
            [r for r in stock_rows if r["total_orgn_amt"] > 0],
            key=lambda x: -x["total_orgn_amt"],
        )[:10]

        # ── 2. 섹터별 누적 순매수 ──────────────
        q_sector = """
            SELECT sector_group,
                   SUM(frgn_net_amt) as total_frgn_amt,
                   SUM(orgn_net_amt) as total_orgn_amt,
                   SUM(frgn_net_amt + orgn_net_amt) as total_net_amt,
                   COUNT(DISTINCT ticker) as stock_count
            FROM supply_demand_daily
            WHERE biz_date >= ? AND sector_group IS NOT NULL AND sector_group != ''
        """
        s_params: list = [cutoff]
        if market != "all":
            q_sector += " AND market = ?"
            s_params.append(market)
        q_sector += " GROUP BY sector_group ORDER BY total_net_amt DESC"

        sector_rows = [dict(r) for r in conn.execute(q_sector, s_params).fetchall()]
        for s in sector_rows:
            s["total_frgn_amt"] = round((s["total_frgn_amt"] or 0) / 100, 1)
            s["total_orgn_amt"] = round((s["total_orgn_amt"] or 0) / 100, 1)
            s["total_net_amt"] = round((s["total_net_amt"] or 0) / 100, 1)

        # ── 3. 요약 ───────────────────────────
        last_date = conn.execute(
            "SELECT MAX(biz_date) as d FROM supply_demand_daily"
        ).fetchone()["d"]

        summary_row = conn.execute(
            """SELECT SUM(frgn_net_amt) as f, SUM(orgn_net_amt) as o,
                      SUM(CASE WHEN frgn_net_amt > 0 AND orgn_net_amt > 0 THEN 1 ELSE 0 END) as dual
               FROM supply_demand_daily WHERE biz_date = ?""",
            (last_date,),
        ).fetchone() if last_date else None

        summary = {
            "foreign_total_amt": round((summary_row["f"] or 0) / 100, 1) if summary_row else 0,
            "institution_total_amt": round((summary_row["o"] or 0) / 100, 1) if summary_row else 0,
            "dual_buy_count": (summary_row["dual"] or 0) if summary_row else 0,
            "last_biz_date": last_date,
        }

        # 데이터 수집 상태
        day_count = conn.execute(
            "SELECT COUNT(DISTINCT biz_date) as c FROM supply_demand_daily"
        ).fetchone()["c"]
        total_rec = conn.execute(
            "SELECT COUNT(*) as c FROM supply_demand_daily"
        ).fetchone()["c"]
    finally:
        conn.close()

    # ── 4. 패턴 감지 ──────────────────────────
    response = {
        "period_days": days,
        "market": market,
        "summary": summary,
        "stock_top_buy": top_buy,
        "stock_top_sell": top_sell,
        "foreign_top": frgn_top,
        "institution_top": orgn_top,
        "sector_summary": sector_rows,
        "sector_rotation": detect_sector_rotation(days),
        "consecutive_buy": detect_consecutive_buying(days),
        "flow_reversals": detect_flow_reversal(days),
        "investor_alignment": detect_investor_alignment(min(days, 5)),
        "data_days": day_count,
        "total_records": total_rec,
    }
    return jsonify(response)


@app.route("/api/supply-demand/collect", methods=["POST"])
def api_supply_demand_collect():
    """수동 수집 트리거 — 1일 1회 제한."""
    global _last_manual_collect
    now = datetime.now()
    if _last_manual_collect and (now - _last_manual_collect).total_seconds() < 86400:
        remaining = 86400 - (now - _last_manual_collect).total_seconds()
        hours = int(remaining // 3600)
        mins = int((remaining % 3600) // 60)
        return jsonify({
            "status": "rate_limited",
            "message": f"1일 1회 제한. {hours}시간 {mins}분 후 가능",
        }), 429

    try:
        saved = collect_daily_supply_demand()
        _last_manual_collect = now
        return jsonify({"status": "ok", "saved": saved})
    except Exception as e:
        logger.exception("수동 수집 실패")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
