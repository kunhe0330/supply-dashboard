# 수급 동향 대시보드

기관/외인 수급 동향을 보여주는 Flask 대시보드.

## 주요 기능
- 매일 15:40 자동 수집 (KIS FHPTJ04400000, 하루 2회 API 호출)
- 종목별 연속 순매수 감지
- 섹터 로테이션 감지
- 수급 전환 신호 감지
- 외인-기관 동조/엇갈림 감지

## 환경변수
- `KIS_APP_KEY`, `KIS_APP_SECRET` — KIS OpenAPI 키
- `DB_PATH` (선택) — SQLite 경로. Railway는 `/app/storage/supply_demand.db`
- `SUPPLY_DEMAND_ENABLED` (선택) — 기본 true

## 로컬 실행
```
pip install -r requirements.txt
python app.py
```
