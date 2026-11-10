# 每日交易批次 Pipeline（Apache Airflow）

每天 00:30（UTC+8）讀取來源系統拋送的 `transactions_YYYYMMDD.csv`，檢查與清洗資料後，依交易日期產出每日成功交易彙總，並回溯重算前兩個交易日。

資料觀察、處理決策與假設請見 [DATA_OBSERVATION.md](DATA_OBSERVATION.md)。

## 環境需求

- Docker 與 Docker Compose v2
- 可用的 8080 port
- 執行測試（選用）：Python 3.10 以上，以及 `pip install pyyaml`

## 使用版本

| 元件 | 版本 |
|---|---|
| Apache Airflow | 2.10.5（LocalExecutor） |
| Python（容器內） | 3.11 |
| PostgreSQL（Airflow metadata） | 16 |

已在 GitHub Codespaces（2 核心，Linux）上驗證。

## 啟動方式

在專案根目錄執行：

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up -d
```

第一行用來讓容器以目前使用者身分寫入檔案，僅 Linux 需要；Windows 或 macOS 使用 Docker Desktop 可略過。

開啟 http://localhost:8080，帳號密碼都是 `airflow`。DAG 名稱為 `daily_transactions`，建立時預設為暫停狀態。

## Pipeline 執行方式

DAG 由五個 task 依序組成，任何一步失敗，後面的 task 都不會執行：

```text
wait_for_file → validate → clean_and_stage → summarize → report
```

| Task | 工作內容 | 程式 |
|---|---|---|
| `wait_for_file` | 等待當日檔案，每 5 分鐘檢查一次，最多等 2 小時 | `dags/daily_transactions.py` |
| `validate` | 檔案大小、資料列、欄位合約，不符合則判定失敗 | `pipeline/validate.py` |
| `clean_and_stage` | 逐筆清洗，不合格的資料列隔離並附上原因 | `pipeline/clean.py` |
| `summarize` | 跨檔案去重、筆數對帳、重算受影響日期 | `pipeline/summarize.py`、`pipeline/dedup.py` |
| `report` | 有隔離、衝突或標記時發出警告 | `pipeline/alerts.py` |

業務規則（門檻、欄位合約、允許的幣別、回溯天數）集中在 `config/data_rules.yaml`。

**處理範例資料**：先把檔案放進 `data/incoming/`，再執行下一節的 backfill。

```bash
cp sample_data/transactions_*.csv data/incoming/
```

**啟用每日排程**：

```bash
docker compose exec airflow-scheduler airflow dags unpause daily_transactions
```

## Backfill / Rerun 執行方式

檔案日期由每次 run 的 `logical_date` 換算成 UTC+8 決定，不使用執行當下的時間，所以補跑舊日期時會讀取該日期的檔案。

**Backfill 一段日期**：

```bash
docker compose exec airflow-scheduler airflow dags backfill daily_transactions \
  -s 2026-06-01 -e 2026-06-05T00:30:00 --continue-on-failures
```

- 排程時間是 00:30，所以結束時間要寫到 `T00:30:00`，才會包含最後一天。
- `--continue-on-failures` 讓某一天失敗後，後面的日期繼續執行。
- 若這段日期已經執行過，請加上 `--reset-dagruns -y`。

**Rerun 單一日期**：

```bash
docker compose exec airflow-scheduler airflow dags backfill daily_transactions \
  -s 2026-06-03T00:30:00 -e 2026-06-03T00:30:00 --reset-dagruns -y
```

重跑會重新計算受影響的日期，結果不會重複累加。來源重送內容不同的檔案時，舊版本的 staging 資料會移到 `data/archive/`，不會被刪除。

**清空產出後重新執行**：

```bash
rm -rf data/staging/* data/output/* data/quarantine/* data/archive/* data/alerts/*
```

## 執行結果驗證方式

本專案已附上 06-01 至 06-05 的執行結果。重新執行後，可用以下方式驗證。

**1. 各日期的執行狀態**

```bash
docker compose exec airflow-scheduler airflow dags list-runs -d daily_transactions
```

| 檔案 | 預期狀態 | 原因 |
|---|---|---|
| 0601、0602、0603 | success | 可正常處理，資料品質問題會發出 WARNING |
| 0604 | failed | 只有 header，沒有資料列 |
| 0605 | failed | 檔案只有 5 bytes，小於 50 bytes |

**2. 彙總結果** `data/output/daily_summary.csv`

| transaction_date | transaction_count | total_amount |
|---|---|---|
| 2026-06-01 | 6 | 3050 |
| 2026-06-02 | 1 | 700 |
| 2026-06-03 | 3 | 2600 |

**3. 筆數對帳**

每次執行的 `audit.json` 都有 `reconciliation`，確認每一筆讀入的資料都有去處，`balanced` 應為 `true`：

```text
讀入筆數 = 隔離 + 計入 + 去重 + 衝突 + 非 SUCCESS
```

**4. 單元測試**（不需要 Airflow）

```bash
python -m unittest discover -s tests -t . -v
```

## 輸出與維運資訊

| 路徑 | 內容 |
|---|---|
| `data/output/daily_summary.csv` | 最新的每日彙總 |
| `data/output/history/` | 每次執行的彙總快照、`audit.json`、`flagged.csv` |
| `data/quarantine/` | 被隔離的資料列（附原因）與 `conflicts.csv` |
| `data/archive/` | 被重送檔案取代的舊版本 |
| `data/alerts/alerts.jsonl` | 所有告警紀錄 |

`audit.json` 記錄來源檔指紋、規則版本、對帳結果，以及每個日期的數字變化。

## 告警機制

| 等級 | 觸發時機 | 內容 |
|---|---|---|
| ERROR | 任何 task 失敗 | DAG、task、run ID、檔案日期、錯誤訊息、log 連結 |
| WARNING | run 成功，但有隔離、衝突或標記 | 對帳結果、隔離原因、衝突 ID、標記統計、數字變化、audit 路徑 |

告警一律寫入 `data/alerts/alerts.jsonl` 與 Airflow log。若要同時發送到 Slack 或 Teams，請在 `docker-compose.yaml` 的 `environment` 加入 `ALERT_WEBHOOK_URL`，設為 incoming webhook 網址後重新啟動容器。

## 專案結構

```text
config/data_rules.yaml   業務規則
dags/                    排程與 task 順序
pipeline/                處理邏輯（不依賴 Airflow，可獨立測試）
tests/                   單元測試
sample_data/             範例輸入檔
data/                    執行時產生的資料
```
