"""
수급 데이터 수집 — KIS FHPTJ04400000 API 호출 + DB 저장
하루 2회 호출 (코스피 + 코스닥).
"""
import logging
import time
from datetime import datetime

import requests

from config import KIS_BASE_URL
from db import get_connection
from kis_auth import get_auth_headers
from sector_loader import get_sector, get_sector_group

logger = logging.getLogger(__name__)


def _safe_int(v, default=0):
    try:
        return int(float(str(v).replace(",", "")))
    except (TypeError, ValueError):
        return default


def _safe_float(v, default=0.0):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return default


def fetch_institution_foreign_top(market_code: str = "0001") -> list[dict]:
    """국내기관_외국인 매매종목가집계 API.

    TR_ID: FHPTJ04400000
    market_code: 0001=코스피, 1001=코스닥
    sort: 순매수상위(0) — 음수 데이터도 함께 반환됨
    """
    tr_id = "FHPTJ04400000"
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/foreign-institution-total"
    params = {
        "FID_COND_MRKT_DIV_CODE": "V",
        "FID_COND_SCR_DIV_CODE": "16449",
        "FID_INPUT_ISCD": market_code,
        "FID_DIV_CLS_CODE": "1",      # 금액정렬
        "FID_RANK_SORT_CLS_CODE": "0",  # 순매수상위
        "FID_ETC_CLS_CODE": "0",       # 전체(외인+기관)
    }
    try:
        resp = requests.get(
            url,
            headers=get_auth_headers(tr_id),
            params=params,
            timeout=10,
        )
        data = resp.json()
    except Exception as e:
        logger.error("수급 API 호출 실패 (%s): %s", market_code, e)
        return []

    if data.get("rt_cd") != "0":
        logger.error(
            "수급 API 에러 (%s): %s %s",
            market_code,
            data.get("msg_cd"),
            data.get("msg1"),
        )
        return []

    output = data.get("output", [])
    logger.info("수급 API %s: %d건", market_code, len(output))
    return output


def _save_records(records: list[dict]) -> int:
    """UPSERT — 같은 biz_date + ticker면 덮어쓰기."""
    if not records:
        return 0
    conn = get_connection()
    now = datetime.now().isoformat()
    try:
        for r in records:
            conn.execute(
                """INSERT INTO supply_demand_daily
                   (collected_at, biz_date, market, ticker, name, sector, sector_group,
                    price, price_change_pct, frgn_net_qty, orgn_net_qty,
                    frgn_net_amt, orgn_net_amt, acml_vol)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(biz_date, ticker) DO UPDATE SET
                     collected_at = excluded.collected_at,
                     market = excluded.market,
                     name = excluded.name,
                     sector = excluded.sector,
                     sector_group = excluded.sector_group,
                     price = excluded.price,
                     price_change_pct = excluded.price_change_pct,
                     frgn_net_qty = excluded.frgn_net_qty,
                     orgn_net_qty = excluded.orgn_net_qty,
                     frgn_net_amt = excluded.frgn_net_amt,
                     orgn_net_amt = excluded.orgn_net_amt,
                     acml_vol = excluded.acml_vol""",
                (
                    now,
                    r["biz_date"],
                    r["market"],
                    r["ticker"],
                    r["name"],
                    r["sector"],
                    r["sector_group"],
                    r["price"],
                    r["price_change_pct"],
                    r["frgn_net_qty"],
                    r["orgn_net_qty"],
                    r["frgn_net_amt"],
                    r["orgn_net_amt"],
                    r["acml_vol"],
                ),
            )
        conn.commit()
        return len(records)
    finally:
        conn.close()


def collect_daily_supply_demand() -> int:
    """일일 수급 데이터 수집 (스케줄러 진입점)."""
    biz_date = datetime.now().strftime("%Y%m%d")
    market_map = {"0001": "KOSPI", "1001": "KOSDAQ"}

    all_records: list[dict] = []

    for market_code, market_name in market_map.items():
        results = fetch_institution_foreign_top(market_code=market_code)

        for item in results:
            ticker = item.get("mksc_shrn_iscd", "") or ""
            if not ticker:
                continue
            ticker = ticker.zfill(6)

            sector = get_sector(ticker)
            record = {
                "biz_date": biz_date,
                "market": market_name,
                "ticker": ticker,
                "name": item.get("hts_kor_isnm", ""),
                "sector": sector,
                "sector_group": get_sector_group(sector) if sector else "기타",
                "price": _safe_int(item.get("stck_prpr")),
                "price_change_pct": _safe_float(item.get("prdy_ctrt")),
                "frgn_net_qty": _safe_int(item.get("frgn_ntby_qty")),
                "orgn_net_qty": _safe_int(item.get("orgn_ntby_qty")),
                "frgn_net_amt": _safe_int(item.get("frgn_ntby_tr_pbmn")),
                "orgn_net_amt": _safe_int(item.get("orgn_ntby_tr_pbmn")),
                "acml_vol": _safe_int(item.get("acml_vol")),
            }
            all_records.append(record)

        time.sleep(1.0)

    saved = _save_records(all_records)
    logger.info("[수급 수집] %s — %d건 저장 완료", biz_date, saved)
    return saved
