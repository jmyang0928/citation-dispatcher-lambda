# 論文引用資訊並行查詢流程

本專案使用 AWS 無伺服器架構，從 S3 上已有的、按月分割的論文資料檔案中，大規模、並行地查詢每篇論文的引用資訊。

## 架構概覽

- **S3 Bucket**: 儲存輸入的論文資料（位於 `cleaned_data/` 前綴下）和最終的查詢結果。
- **Dispatcher Lambda**: **掃描 `cleaned_data/` 資料夾，逐一讀取每個 `.jsonl` 檔案**，將每篇論文轉換為一個任務，並發送到 SQS 佇列。這種方法避免了一次性載入大檔案導致的記憶體問題。
- **SQS Queue**: 作為任務緩衝區，儲存數百萬個待處理的查詢任務，並以受控的速率觸發 Worker。
- **Worker Lambda**: 接收單一論文的查詢任務，透過 `semanticscholar` 函式庫與 Semantic Scholar API 互動，並將結果（成功、找不到、或錯誤）寫回 S3。

## 檔案結構

在部署前，請按以下結構組織您的專案檔案：

```
.
├── src/
│   ├── dispatcher_citation/
│   │   ├── citation_dispatcher_lambda.py
│   │   └── requirements.txt  # (pandas)
│   └── worker_citation/
│       ├── citation_worker_lambda.py
│       └── requirements.txt  # (semanticscholar)
│├── template.yaml                 # AWS SAM 部署範本
└── README.md                     # 本文件
```

**前置作業**: 請確保您之前步驟產生的、清理過的 `.jsonl` 檔案已存在於 S3 儲存桶的 `cleaned_data/` 資料夾中。

## 操作流程

### 步驟 1：部署

1.  **前提條件**: 確認已安裝 AWS CLI, AWS SAM CLI, 和 Docker。
2.  **建置**: `sam build --use-container`
3.  **部署**: `sam deploy --guided`

### 步驟 2：啟動查詢任務

1.  登入 AWS 管理控制台，導航至 **Lambda** 服務。
2.  找到您部署的 **CitationDispatcherFunction**。
3.  **手動觸發 (Invoke)** 該函式一次。
4.  您可以在該函式的 CloudWatch Logs 中觀察進度。它會顯示正在處理哪個 `.jsonl` 檔案，以及已分派了多少任務。

### 步驟 3：監控進度

-   在 **SQS** 控制台，選擇 `CitationJobQueue` 佇列。您會看到「可用訊息」的數量快速增加到您論文的總數。
-   隨著 Worker Lambda 的執行，這個數字會逐漸減少。**這個過程可能會持續數小時甚至數天**，但它會在背景全自動、可靠地執行。
-   您可以隨時檢查 S3 儲存桶，查看 `citation_results/` 資料夾下的產出。

### 最終結果

所有流程完成後（即 SQS 佇列清空），您將在 S3 儲存桶中獲得以下產出：

-   `citation_results/`:
    -   `success/`: 包含所有成功查詢到的引用資訊。
    -   `not_found/`: 記錄所有在 Semantic Scholar 中找不到的論文。
    -   `error/`: 記錄所有在查詢過程中發生錯誤的論文。
