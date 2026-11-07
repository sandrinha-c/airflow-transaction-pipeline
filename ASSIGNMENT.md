# Data Engineer 面試作業：Airflow Data Pipeline

## 題目情境

某來源系統每日於 00:00（UTC+8）拋送一份交易資料 CSV 檔案，檔名格式如下：

`transactions_YYYYMMDD.csv`

其中 `YYYYMMDD` 代表檔案拋送日期，例如，2026/06/03 拋送的檔案名稱為：`transactions_20260603.csv`

Airflow Pipeline 每日於 00:30（UTC+8）開始執行，讀取當日拋送的交易檔案

請使用 Apache Airflow 設計並實作一條每日批次資料處理 Pipeline

## 作業需求

Pipeline 需完成以下處理：

1. 針對輸入資料進行必要的資料檢查與處理。若發生以下任一情況，排程應判定為失敗，並觸發告警機制：

   - file_size < 50 bytes
   - header 以外的資料不得為空

2. 依交易日期產出「每日成功交易彙總」資料，至少包含以下欄位：

   - `transaction_date`
   - `transaction_count`
   - `total_amount`

3. DAG 每次執行時，需重新處理當次涉及的交易日期，並回溯重新計算前兩個交易日的彙總結果

4. Pipeline 需支援指定排程日期重新執行（backfill / rerun）

5. 當排程有問題時，請設計告警機制，並保留你認為有助於問題追蹤或日常維運的資訊

## 技術環境

- Python 版本需為 3.10 以上
- Apache Airflow 2.x 版本

## 交付內容

請提供以下內容：

- Airflow DAG 與相關程式碼
- 可重現的執行方式
- Pipeline 執行後的 output data
- README，至少包含
  - 環境需求
  - 使用版本
  - 啟動方式
  - Pipeline 執行方式
  - Backfill / rerun 執行方式
  - 執行結果驗證方式
- Data Observation / Assumption，至少說明：
  - 對輸入資料的觀察
  - 發現的資料問題或特殊情況
  - 處理時所做的假設
  - 對應的處理方式與原因

## 其他說明

- 不需串接真實雲端服務或公司內部資源
- 可使用 Docker 或本機 Airflow 環境
- 可自行選擇 Python 套件及資料儲存方式
- 可使用 AI 工具輔助開發，但需能說明 Pipeline 設計及資料處理決策
