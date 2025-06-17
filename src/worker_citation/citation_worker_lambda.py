import os
import json
import time
import requests
import boto3
import logging
from urllib.parse import quote_plus
from typing import Dict, Any, List
import secrets

# --- Configuration ---
# Lambda 環境變數
BUCKET = os.environ['BUCKET']
OUTPUT_PREFIX = os.environ['OUTPUT_PREFIX']
SUCCESS_FOLDER = os.environ.get('SUCCESS_FOLDER', 'success/')
FAILURE_FOLDER = os.environ.get('FAILURE_FOLDER', 'failure/')
OPENALEX_EMAIL = os.environ['OPENALEX_EMAIL']

# Boto3 and other clients
s3_client = boto3.client('s3')
session = requests.Session() # 使用 Session 提升性能

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Rate Limiter ---
# OpenAlex 禮貌性請求池 (polite pool) 速率為 10 req/s
# 為確保多個 Worker Lambda 併發執行時總速率不超標，
# 我們在單一 Worker 內部進行速率控制。
# 每次 API call 後等待一小段時間。
# 若 Lambda 併發數設為 10，則 REQUEST_INTERVAL_SECONDS 設為 1.0 秒，
# 總速率就會被控制在 10 req/s 左右。
# 這裡設定一個保守值。
REQUEST_INTERVAL_SECONDS = 0.2 # 5 req/s per worker

class RateLimiter:
    """A simple rate limiter using time.perf_counter for precision."""
    def __init__(self, interval_seconds: float):
        self.interval = interval_seconds
        self.last_call_time = 0

    def wait(self):
        """Waits if the time since the last call is less than the interval."""
        elapsed = time.perf_counter() - self.last_call_time
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self.last_call_time = time.perf_counter()

# 初始化全域速率控制器
rate_limiter = RateLimiter(REQUEST_INTERVAL_SECONDS)

def lambda_handler(event: Dict[str, Any], context: object) -> None:
    """
    Processes a batch of records from S3, queries OpenAlex,
    and saves results back to S3.
    """
    for record in event['Records']:
        message_body = json.loads(record['body'])
        batch_s3_key = message_body['s3_key']
        batch_id = message_body.get('batch_id', 'N/A')
        logger.info(f"Processing batch_id: {batch_id} from s3://{BUCKET}/{batch_s3_key}")

        try:
            # 下載 S3 批次檔
            s3_object = s3_client.get_object(Bucket=BUCKET, Key=batch_s3_key)
            lines = s3_object['Body'].read().decode('utf-8').splitlines()

            for line in lines:
                if not line.strip():
                    continue
                try:
                    original_record = json.loads(line)
                    process_single_record(original_record)
                except json.JSONDecodeError:
                    logger.error(f"Skipping invalid JSON line in batch {batch_id}: {line}")
                except Exception as e:
                    # 處理單筆紀錄時發生非預期錯誤
                    logger.error(f"Error processing record in batch {batch_id}: {line}, Error: {e}")
                    # 可以選擇將錯誤紀錄也存到 failure
                    record_id = json.loads(line).get('id', 'unknown_id')
                    failure_payload = {
                        "id": record_id,
                        "title": json.loads(line).get('title', 'unknown_title'),
                        "error": f"Internal processing error: {str(e)}"
                    }
                    save_to_s3(failure_payload, record_id, is_success=False)

        except Exception as e:
            # 處理整個批次檔時發生錯誤（如下載失敗），此訊息會被重試或進入 DLQ
            logger.error(f"FATAL: Could not process batch {batch_s3_key}. Error: {e}")
            raise e # 拋出異常，讓 SQS 知道此訊息處理失敗


def process_single_record(record: Dict[str, Any]):
    """
    Queries OpenAlex for a single record and saves the result.
    """
    arxiv_id = record.get('id')
    title = record.get('title')

    if not arxiv_id or not title:
        logger.warning(f"Skipping record with missing id or title: {record}")
        return

    try:
        # 1. 查詢 OpenAlex Works API
        # 使用 quote_plus 確保 title 中的特殊字元被正確編碼
        encoded_title = quote_plus(f'"{title}"')
        works_url = f"https://api.openalex.org/works?search={encoded_title}&per_page=1&mailto={secrets.token_hex(4) + "@example.com"}"

        rate_limiter.wait() # 查詢前等待，確保速率
        response = session.get(works_url, timeout=15)
        response.raise_for_status() # 確保請求成功 (2xx)
        works_data = response.json()

        if not works_data.get('results'):
            raise ValueError("No results found in OpenAlex for the given title.")

        work_result = works_data['results'][0]

        # 2. 提取 Work 資訊
        record["oa_display_name"] = work_result.get("display_name")
        record["cited_by_count"] = work_result.get("cited_by_count", 0)

        # 3. 查詢作者 h-index
        authorships = work_result.get("authorships", [])
        authors_hindex = []

        for authorship in authorships:
            author_info = authorship.get("author", {})
            author_id = author_info.get("id")
            author_name = author_info.get("display_name")

            if author_id:
                try:
                    author_url = f"{author_id}?mailto={secrets.token_hex(4) + "@example.com"}"
                    rate_limiter.wait() # 查詢前等待，確保速率
                    author_res = session.get(author_url, timeout=10)
                    author_res.raise_for_status()
                    author_data = author_res.json()
                    h_index = author_data.get("summary_stats", {}).get("h_index", 0)
                    authors_hindex.append({"name": author_name, "h_index": h_index})
                except requests.exceptions.RequestException as author_e:
                    logger.warning(f"Could not fetch h-index for author {author_id}: {author_e}")
                    authors_hindex.append({"name": author_name, "h_index": None}) # 紀錄查詢失敗

        record["authors_hindex"] = authors_hindex

        # 4. 儲存成功結果
        save_to_s3(record, arxiv_id, is_success=True)
        logger.info(f"Successfully processed and saved record: {arxiv_id}")

    except Exception as e:
        # 處理所有查詢失敗的情況
        logger.error(f"Failed to process record {arxiv_id} ('{title}'). Error: {str(e)}")
        failure_payload = {
            "id": arxiv_id,
            "title": title,
            "error": str(e)
        }
        save_to_s3(failure_payload, arxiv_id, is_success=False)


def save_to_s3(payload: Dict[str, Any], file_id: str, is_success: bool):
    """Saves the final payload to the appropriate S3 folder."""
    folder = SUCCESS_FOLDER if is_success else FAILURE_FOLDER
    # 清理 file_id，避免路徑遍歷問題
    safe_file_id = file_id.replace('/', '_')
    output_key = f"{OUTPUT_PREFIX}{folder}{safe_file_id}.json" # 注意，存為 .json 而非 .jsonl

    s3_client.put_object(
        Bucket=BUCKET,
        Key=output_key,
        Body=json.dumps(payload, ensure_ascii=False),
        ContentType='application/json'
    )