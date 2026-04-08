"""
KIS API 인증 — 파일 캐시로 1일 1회 발급 준수
"""
import json
import os
import time
import logging
from datetime import datetime, timedelta
from threading import Lock

import requests

from config import KIS_BASE_URL, KIS_APP_KEY, KIS_APP_SECRET, DB_PATH

logger = logging.getLogger(__name__)

_token_lock = Lock()
_access_token: str = ""
_token_expires_at: datetime = datetime.min

# 토큰 캐시 파일 (Railway 볼륨)
_CACHE_DIR = os.path.dirname(DB_PATH) or "storage"
_TOKEN_CACHE_PATH = os.path.join(_CACHE_DIR, ".kis_token_cache.json")


def _save_token_cache(token: str, expires_at: datetime) -> None:
    try:
        os.makedirs(_CACHE_DIR, exist_ok=True)
        with open(_TOKEN_CACHE_PATH, "w") as f:
            json.dump({
                "access_token": token,
                "expires_at": expires_at.isoformat(),
            }, f)
        logger.info("토큰 캐시 저장: %s", _TOKEN_CACHE_PATH)
    except Exception as e:
        logger.warning("토큰 캐시 저장 실패: %s", e)


def _load_token_cache():
    try:
        if not os.path.exists(_TOKEN_CACHE_PATH):
            return None
        with open(_TOKEN_CACHE_PATH, "r") as f:
            cache = json.load(f)
        token = cache["access_token"]
        expires_at = datetime.fromisoformat(cache["expires_at"])
        if datetime.now() >= expires_at:
            logger.info("캐시 토큰 만료")
            return None
        logger.info("캐시 토큰 로드, 만료: %s", expires_at.isoformat())
        return token, expires_at
    except Exception as e:
        logger.warning("캐시 로드 실패: %s", e)
        return None


def _issue_token():
    url = f"{KIS_BASE_URL}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
    }
    resp = requests.post(url, json=body, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    token = data["access_token"]
    expires_in = int(data.get("expires_in", 86400))
    expires_at = datetime.now() + timedelta(seconds=expires_in - 3600)
    logger.info("KIS 토큰 신규 발급, 만료: %s", expires_at.isoformat())
    _save_token_cache(token, expires_at)
    return token, expires_at


def get_access_token() -> str:
    """토큰 반환. 메모리 → 파일 → 신규발급."""
    global _access_token, _token_expires_at
    with _token_lock:
        if _access_token and datetime.now() < _token_expires_at:
            return _access_token
        cached = _load_token_cache()
        if cached:
            _access_token, _token_expires_at = cached
            return _access_token
        logger.info("유효한 토큰 없음, 신규 발급")
        for attempt in range(3):
            try:
                _access_token, _token_expires_at = _issue_token()
                break
            except Exception as e:
                logger.warning("토큰 발급 실패 (%d/3): %s", attempt + 1, e)
                if attempt < 2:
                    time.sleep(2)
                else:
                    raise
        return _access_token


def get_auth_headers(tr_id: str) -> dict:
    return {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {get_access_token()}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": tr_id,
    }
