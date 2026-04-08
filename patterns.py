"""
패턴 감지 엔진 —
(1) 연속 순매수
(2) 섹터 로테이션
(3) 수급 전환 신호
(4) 외인-기관 동조/엇갈림

금액 단위: DB는 백만원 저장 → 결과는 억원(/100)으로 변환
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from db import get_connection

logger = logging.getLogger(__name__)

MIN_STREAK_DAYS = 3  # 연속 순매수 최소 일수


# ────────────────────────────────────────────────────────
# 공통 쿼리 유틸
# ────────────────────────────────────────────────────────
def _cutoff_date(days: int) -> str:
    """영업일 보정 (주말/공휴일) 위해 days * 1.5 일 전."""
    return (datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y%m%d")


# ────────────────────────────────────────────────────────
# 1. 연속 순매수 감지
# ────────────────────────────────────────────────────────
def detect_consecutive_buying(days: int = 20) -> list[dict]:
    """종목별 최근 연속 순매수일(합산 기준) 감지. streak >= 3일만."""
    conn = get_connection()
    try:
        cutoff = _cutoff_date(days)
        rows = conn.execute(
            """SELECT biz_date, ticker, name, sector_group, market,
                      frgn_net_amt, orgn_net_amt
               FROM supply_demand_daily
               WHERE biz_date >= ?
               ORDER BY ticker, biz_date DESC""",
            (cutoff,),
        ).fetchall()

        by_ticker: dict[str, list] = defaultdict(list)
        for r in rows:
            by_ticker[r["ticker"]].append(dict(r))

        results = []
        for ticker, recs in by_ticker.items():
            # 최근 → 과거 순 (이미 DESC)
            streak = 0
            total_amt = 0
            frgn_pos_days = 0
            orgn_pos_days = 0
            start_date = None
            for rec in recs:
                net = (rec["frgn_net_amt"] or 0) + (rec["orgn_net_amt"] or 0)
                if net > 0:
                    streak += 1
                    total_amt += net
                    start_date = rec["biz_date"]
                    if (rec["frgn_net_amt"] or 0) > 0:
                        frgn_pos_days += 1
                    if (rec["orgn_net_amt"] or 0) > 0:
                        orgn_pos_days += 1
                else:
                    break

            if streak < MIN_STREAK_DAYS:
                continue

            # 투자자 구분
            if frgn_pos_days >= streak and orgn_pos_days >= streak:
                investor = "both"
            elif frgn_pos_days >= streak:
                investor = "foreign"
            elif orgn_pos_days >= streak:
                investor = "institution"
            else:
                investor = "mixed"

            first = recs[0]
            results.append({
                "ticker": ticker,
                "name": first["name"],
                "sector_group": first["sector_group"],
                "market": first["market"],
                "streak_days": streak,
                "total_net_amt": round(total_amt / 100, 1),   # 억원
                "avg_daily_amt": round((total_amt / streak) / 100, 1),  # 억원
                "streak_start": start_date,
                "investor": investor,
            })

        results.sort(key=lambda x: (-x["streak_days"], -x["total_net_amt"]))
        return results
    finally:
        conn.close()


# ────────────────────────────────────────────────────────
# 2. 섹터 로테이션 감지
# ────────────────────────────────────────────────────────
def detect_sector_rotation(days: int = 20) -> dict:
    """최근 5영업일 vs 이전 5영업일 섹터별 순매수 비교."""
    conn = get_connection()
    try:
        # 최근 10일치 영업일 목록
        dates = [r["biz_date"] for r in conn.execute(
            """SELECT DISTINCT biz_date FROM supply_demand_daily
               ORDER BY biz_date DESC LIMIT 10"""
        ).fetchall()]
        if len(dates) < 6:
            return {"rotating_in": [], "rotating_out": [],
                    "steady_in": [], "steady_out": [],
                    "note": "섹터 로테이션은 최소 10일 데이터 필요"}

        recent_dates = dates[:5]
        prev_dates = dates[5:10] if len(dates) >= 10 else dates[5:]

        def _sum_by_sector(date_list):
            if not date_list:
                return {}
            placeholders = ",".join("?" * len(date_list))
            rows = conn.execute(
                f"""SELECT sector_group,
                          SUM(frgn_net_amt + orgn_net_amt) as total
                   FROM supply_demand_daily
                   WHERE biz_date IN ({placeholders})
                         AND sector_group IS NOT NULL AND sector_group != ''
                   GROUP BY sector_group""",
                date_list,
            ).fetchall()
            return {r["sector_group"]: (r["total"] or 0) for r in rows}

        recent = _sum_by_sector(recent_dates)
        prev = _sum_by_sector(prev_dates)

        all_sectors = set(recent.keys()) | set(prev.keys())
        rotating_in = []
        rotating_out = []
        steady_in = []
        steady_out = []

        for s in all_sectors:
            r_amt = recent.get(s, 0)
            p_amt = prev.get(s, 0)
            if p_amt == 0:
                change_pct = 100.0 if r_amt > 0 else (-100.0 if r_amt < 0 else 0)
            else:
                change_pct = (r_amt - p_amt) / abs(p_amt) * 100

            entry = {
                "sector": s,
                "recent_amt": round(r_amt / 100, 1),   # 억원
                "prev_amt": round(p_amt / 100, 1),
                "change_pct": round(change_pct, 1),
            }

            if r_amt > 0 and change_pct > 50:
                entry["label"] = "🔺 유입 가속"
                rotating_in.append(entry)
            elif r_amt > 0 and p_amt <= 0:
                entry["label"] = "🆕 유입 전환"
                rotating_in.append(entry)
            elif r_amt < 0 and p_amt > 0:
                entry["label"] = "🔻 유출 전환"
                rotating_out.append(entry)
            elif r_amt > 0 and change_pct < -50:
                entry["label"] = "📉 유입 둔화"
                steady_in.append(entry)
            elif r_amt > 0:
                entry["label"] = "➡️ 유입 유지"
                steady_in.append(entry)
            elif r_amt < 0:
                entry["label"] = "⬇️ 유출 지속"
                steady_out.append(entry)

        rotating_in.sort(key=lambda x: -x["recent_amt"])
        rotating_out.sort(key=lambda x: x["recent_amt"])
        steady_in.sort(key=lambda x: -x["recent_amt"])
        steady_out.sort(key=lambda x: x["recent_amt"])

        return {
            "rotating_in": rotating_in,
            "rotating_out": rotating_out,
            "steady_in": steady_in,
            "steady_out": steady_out,
        }
    finally:
        conn.close()


# ────────────────────────────────────────────────────────
# 3. 수급 전환 신호 감지
# ────────────────────────────────────────────────────────
def detect_flow_reversal(days: int = 20) -> list[dict]:
    """이전 5일 중 4일 이상 순매도 → 최근 3일 연속 순매수 종목."""
    conn = get_connection()
    try:
        dates = [r["biz_date"] for r in conn.execute(
            """SELECT DISTINCT biz_date FROM supply_demand_daily
               ORDER BY biz_date DESC LIMIT 8"""
        ).fetchall()]
        if len(dates) < 8:
            return []

        recent_3 = dates[:3]
        prev_5 = dates[3:8]

        # 최근 3일 연속 순매수 종목
        placeholders = ",".join("?" * 3)
        recent_rows = conn.execute(
            f"""SELECT ticker, name, sector_group, market,
                       SUM(frgn_net_amt) as frgn_sum,
                       SUM(orgn_net_amt) as orgn_sum,
                       SUM(frgn_net_amt + orgn_net_amt) as total_sum,
                       COUNT(*) as cnt
                FROM supply_demand_daily
                WHERE biz_date IN ({placeholders})
                GROUP BY ticker
                HAVING cnt >= 3 AND total_sum > 0""",
            recent_3,
        ).fetchall()

        results = []
        prev_ph = ",".join("?" * len(prev_5))
        for rec in recent_rows:
            ticker = rec["ticker"]
            prev_data = conn.execute(
                f"""SELECT SUM(frgn_net_amt) as frgn_sum,
                           SUM(orgn_net_amt) as orgn_sum,
                           SUM(CASE WHEN frgn_net_amt + orgn_net_amt < 0 THEN 1 ELSE 0 END) as sell_days
                    FROM supply_demand_daily
                    WHERE ticker = ? AND biz_date IN ({prev_ph})""",
                (ticker, *prev_5),
            ).fetchone()

            if not prev_data or (prev_data["sell_days"] or 0) < 4:
                continue

            prev_total = (prev_data["frgn_sum"] or 0) + (prev_data["orgn_sum"] or 0)
            if prev_total >= 0:
                continue  # 이전 기간 합산도 마이너스여야 전환

            # 누가 전환했나
            frgn_rev = (prev_data["frgn_sum"] or 0) < 0 and (rec["frgn_sum"] or 0) > 0
            orgn_rev = (prev_data["orgn_sum"] or 0) < 0 and (rec["orgn_sum"] or 0) > 0

            if frgn_rev and orgn_rev:
                reversal_type = "both"
                label = "🔄 외인+기관 매수 전환"
            elif frgn_rev:
                reversal_type = "foreign"
                label = "🔄 외인 매수 전환"
            elif orgn_rev:
                reversal_type = "institution"
                label = "🔄 기관 매수 전환"
            else:
                continue

            results.append({
                "ticker": ticker,
                "name": rec["name"],
                "sector_group": rec["sector_group"],
                "market": rec["market"],
                "prev_5d_amt": round(prev_total / 100, 1),
                "recent_3d_amt": round((rec["total_sum"] or 0) / 100, 1),
                "reversal_type": reversal_type,
                "label": label,
            })

        results.sort(key=lambda x: -x["recent_3d_amt"])
        return results
    finally:
        conn.close()


# ────────────────────────────────────────────────────────
# 4. 외인-기관 동조/엇갈림 감지
# ────────────────────────────────────────────────────────
def detect_investor_alignment(days: int = 5) -> list[dict]:
    """최근 N일 외인 vs 기관 합산 비교 — 쌍끌이 매수/매도, 엇갈림 감지."""
    conn = get_connection()
    try:
        dates = [r["biz_date"] for r in conn.execute(
            """SELECT DISTINCT biz_date FROM supply_demand_daily
               ORDER BY biz_date DESC LIMIT ?""",
            (days,),
        ).fetchall()]
        if len(dates) < 3:
            return []

        placeholders = ",".join("?" * len(dates))
        rows = conn.execute(
            f"""SELECT ticker, name, sector_group, market,
                       SUM(frgn_net_amt) as frgn_sum,
                       SUM(orgn_net_amt) as orgn_sum
                FROM supply_demand_daily
                WHERE biz_date IN ({placeholders})
                GROUP BY ticker""",
            dates,
        ).fetchall()

        results = []
        for r in rows:
            frgn = r["frgn_sum"] or 0
            orgn = r["orgn_sum"] or 0

            if frgn > 0 and orgn > 0:
                alignment = "쌍끌이 매수"
                label = "🔥"
            elif frgn < 0 and orgn < 0:
                alignment = "쌍끌이 매도"
                label = "❄️"
            elif frgn > 0 and orgn < 0:
                alignment = "외인↑ 기관↓"
                label = "⚡"
            elif frgn < 0 and orgn > 0:
                alignment = "외인↓ 기관↑"
                label = "⚡"
            else:
                continue

            results.append({
                "ticker": r["ticker"],
                "name": r["name"],
                "sector_group": r["sector_group"],
                "market": r["market"],
                "frgn_5d_amt": round(frgn / 100, 1),
                "orgn_5d_amt": round(orgn / 100, 1),
                "alignment": alignment,
                "label": label,
            })

        # 쌍끌이 매수를 가장 위에, 금액 큰 순
        results.sort(key=lambda x: (
            0 if x["alignment"] == "쌍끌이 매수" else
            1 if x["alignment"] == "외인↑ 기관↓" else
            2 if x["alignment"] == "외인↓ 기관↑" else 3,
            -(x["frgn_5d_amt"] + x["orgn_5d_amt"]),
        ))
        return results
    finally:
        conn.close()
