# Deal History Archive (SQLite)

매주 월요일 자동 누적되는 raw deal 스냅샷 아카이브.

## 구조

- **위치**: GitHub Release `history-archive` 의 `history.sqlite` asset
- **스키마**: `deal_history` 테이블, PK = (as_of_date, deal_id, source)
- **누적 방식**: 매주 전체 deal 스냅샷 append (이전 주 데이터 유지)
- **메타**: `archive_runs` 테이블에 각 주차 실행 로그

## 다운로드

```bash
gh release download history-archive -p history.sqlite -D /tmp/
```

## 쿼리 예시

```bash
sqlite3 /tmp/history.sqlite
```

### 1. 누적 주차 확인

```sql
SELECT as_of_date, n_deals, n_indiv, n_corp FROM archive_runs ORDER BY as_of_date;
```

### 2. 특정 시점의 pipeline 분포 재현

```sql
SELECT pipeline, status, COUNT(*) AS n, ROUND(SUM(apply_amount)/1e8, 1) AS apply_억
FROM deal_history
WHERE as_of_date = '2026-04-17'
  AND apply_date >= '2025-11-01'
  AND apply_date < '2025-11-30'
GROUP BY pipeline, status
ORDER BY apply_억 DESC;
```

### 3. 특정 deal의 상태 변화 추적

```sql
SELECT as_of_date, status, pipeline, apply_amount, payment_amount, lost_reason
FROM deal_history
WHERE deal_id = 12345 AND source = 'indiv'
ORDER BY as_of_date;
```

### 4. 주차별 A(지수) 비중 변화

```sql
SELECT as_of_date,
       ROUND(SUM(CASE WHEN pipeline = 'A(지수)' THEN apply_amount ELSE 0 END) / SUM(apply_amount) * 100, 2) AS a_jisu_share_pct,
       ROUND(SUM(apply_amount) / 1e8, 1) AS total_apply_억
FROM deal_history
WHERE apply_date BETWEEN '2024-11-01' AND '2025-10-31'
GROUP BY as_of_date
ORDER BY as_of_date;
```

### 5. 과거 임의 시점의 "실패 사유 Top 10"

```sql
SELECT lost_reason, COUNT(*) AS n, ROUND(SUM(apply_amount)/1e8, 1) AS apply_억
FROM deal_history
WHERE as_of_date = '2026-01-06'  -- 1월 초 스냅샷
  AND status = '실패'
  AND apply_date >= '2025-01-01'
GROUP BY lost_reason
ORDER BY apply_억 DESC
LIMIT 10;
```

### 6. Python 분석 (DuckDB or pandas)

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('/tmp/history.sqlite')
df = pd.read_sql("""
  SELECT as_of_date, pipeline, SUM(apply_amount)/1e8 AS apply_억
  FROM deal_history
  GROUP BY as_of_date, pipeline
""", conn)
```

## 크기 예상

- 주당 ~240k 행 추가 (개인 233k + 법인 8k)
- 연간 누적: ~12M 행
- SQLite 파일 크기: 주당 ~30~50MB, 연간 ~500MB~1.5GB (압축 전)
- GitHub Release 한 파일 최대 2GB — 1년 여유, 그 이후엔 연간 분할 가능

## 초기화 (full sync 재시작)

만약 schema 변경 등으로 초기화가 필요하면:

```bash
gh release delete history-archive -y
# 다음 workflow 실행 시 빈 상태에서 다시 시작
```
