"""
상장법인목록.xls + sector_map.json으로 종목코드 → (업종, 대분류) 매핑 로드
앱 시작 시 1회 로드하여 메모리에 보관.
"""
import json
import logging
import os
import re

logger = logging.getLogger(__name__)

SECTOR_MAP: dict[str, str] = {}       # 종목코드 → 업종(세분류)
SECTOR_GROUP_MAP: dict[str, str] = {}  # 업종(세분류) → 대분류
TICKER_NAME_MAP: dict[str, str] = {}   # 종목코드 → 회사명

_HERE = os.path.dirname(os.path.abspath(__file__))
_XLS_PATH = os.path.join(_HERE, "상장법인목록.xls")
_MAP_PATH = os.path.join(_HERE, "sector_map.json")


def load_sector_data():
    """상장법인목록 + sector_map 로드. 반환: (로드된 종목 수, 대분류 수)"""
    global SECTOR_MAP, SECTOR_GROUP_MAP, TICKER_NAME_MAP

    # 1) sector_map.json 로드
    with open(_MAP_PATH, "r", encoding="utf-8") as f:
        sm_data = json.load(f)
    SECTOR_GROUP_MAP = sm_data["mapping"]

    # 2) 상장법인목록.xls 로드
    if not os.path.exists(_XLS_PATH):
        logger.warning("상장법인목록.xls 없음: %s", _XLS_PATH)
        return 0, len(set(SECTOR_GROUP_MAP.values()))

    import pandas as pd
    dfs = pd.read_html(_XLS_PATH, encoding="cp949")
    df = dfs[0]
    df.columns = ["회사명", "종목구분", "종목코드", "업종", "주요제품",
                  "상장일", "결산", "대표자명", "홈페이지", "지역"]
    df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)
    df = df[df["종목코드"].str.match(r"^\d{6}$")]

    for _, row in df.iterrows():
        code = row["종목코드"]
        SECTOR_MAP[code] = row["업종"] if pd.notna(row["업종"]) else ""
        TICKER_NAME_MAP[code] = row["회사명"] if pd.notna(row["회사명"]) else ""

    logger.info(
        "섹터 매핑 로드: %d개 종목, %d개 세분류, %d개 대분류",
        len(SECTOR_MAP),
        len(SECTOR_GROUP_MAP),
        len(set(SECTOR_GROUP_MAP.values())),
    )
    return len(SECTOR_MAP), len(set(SECTOR_GROUP_MAP.values()))


def get_sector(ticker: str) -> str:
    """종목코드 → 세분류 업종명."""
    return SECTOR_MAP.get(ticker.zfill(6), "")


def get_sector_group(ticker_or_sector: str) -> str:
    """종목코드(6자리) 또는 세분류 업종명 → 대분류."""
    if re.match(r"^\d{6}$", ticker_or_sector):
        sector = SECTOR_MAP.get(ticker_or_sector, "")
    else:
        sector = ticker_or_sector
    return SECTOR_GROUP_MAP.get(sector, "기타")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_sector_data()
    # 샘플
    for tk in ["005930", "000660", "035720", "207940"]:
        print(f"{tk}: {TICKER_NAME_MAP.get(tk)} / {get_sector(tk)} / {get_sector_group(tk)}")
