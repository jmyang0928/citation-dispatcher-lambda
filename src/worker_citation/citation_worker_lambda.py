import json
import os
import boto3
import traceback
import requests
import urllib.parse

# 從環境變數獲取配置
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
OPENALEX_EMAIL = os.environ.get('OPENALEX_EMAIL', 'default-user@example.com')

if not S3_BUCKET_NAME:
    raise ValueError("Environment variable S3_BUCKET_NAME is not set")

# 初始化 S3 客戶端
s3 = boto3.client('s3')

def handler(event, context):
    """
    Lambda 處理函式，由 SQS 觸發。此函式現在會處理一個批次的訊息。
    """
    batch_size = len(event.get('Records', []))
    print(f"Received a batch of {batch_size} records.")
    
    for record in event.get('Records', []):
        paper_id = 'unknown_id'
        original_message = record.get('body', '{}')
        
        try:
            message_body = json.loads(original_message)
            paper_id = message_body.get('paper_id')
            title = message_body.get('title')

            if not paper_id or not title:
                print(f"Invalid message, missing paper_id or title: {original_message}")
                log_permanent_error(paper_id, 'MalformedMessage', 'Message is missing paper_id or title', original_message)
                continue

        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON message body: {e}")
            log_permanent_error('unknown_id_parsing_error', type(e).__name__, str(e), original_message)
            continue

        print(f"Processing Paper ID: {paper_id}")
        process_paper(paper_id, title, original_message)


def process_paper(paper_id: str, title: str, original_message: str):
    """
    使用 OpenAlex API 處理單篇論文的查詢邏輯，並將結果寫入 S3。
    """
    try:
        # 步驟 1: 在 OpenAlex 中搜尋論文
        safe_title = urllib.parse.quote_plus(f'"{title}"')
        works_url = f"https://api.openalex.org/works?search={safe_title}&per_page=1&mailto={OPENALEX_EMAIL}"
        
        r_works = requests.get(works_url, timeout=30)
        r_works.raise_for_status()

        works_data = r_works.json()

        if not works_data.get("results"):
            log_not_found(paper_id, title, 'No results returned from OpenAlex.')
            return

        paper_data = works_data["results"][0]

        # 步驟 2: 提取作者 ID 並批次獲取其詳細資訊 (包含 H-index)
        author_info = []
        author_ids = [
            authorship['author']['id'].split('/')[-1] 
            for authorship in paper_data.get('authorships', []) 
            if authorship.get('author') and authorship['author'].get('id')
        ]
        
        if author_ids:
            author_id_filter = '|'.join(author_ids)
            authors_url = f"https://api.openalex.org/authors?filter=openalex_id:{author_id_filter}&per_page={len(author_ids)}&mailto={OPENALEX_EMAIL}"
            
            r_authors = requests.get(authors_url, timeout=30)
            r_authors.raise_for_status()
            authors_data = r_authors.json()
            
            author_details_map = {author['id'].split('/')[-1]: author for author in authors_data.get('results', [])}
            
            for authorship in paper_data.get('authorships', []):
                 if authorship.get('author') and authorship['author'].get('id'):
                    auth_id = authorship['author']['id'].split('/')[-1]
                    details = author_details_map.get(auth_id)
                    display_name = authorship.get('author', {}).get('display_name')
                    h_index = details.get('summary_stats', {}).get('h_index') if details else None
                    author_info.append({'name': display_name, 'hIndex': h_index})

        # 步驟 3: 組合並儲存成功結果
        result = {
            'original_id': paper_id,
            'searched_title': title,
            'found_paper_title': paper_data.get('display_name'),
            'citationCount': paper_data.get('cited_by_count'),
            'authors': author_info
        }
        
        s3_key = f"citation_results/success/{paper_id}.json"
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(result, indent=2, ensure_ascii=False))
        print(f"SUCCESS: Paper ID {paper_id} processed and saved to {s3_key}")

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        if status_code == 429 or status_code >= 500:
            handle_retriable_error(e, paper_id, title, original_message)
        elif status_code == 404:
            log_not_found(paper_id, title, f'OpenAlex API returned HTTP 404.')
        else:
            log_permanent_error(paper_id, f"HTTPError_{status_code}", str(e), original_message)

    except Exception as e:
        handle_retriable_error(e, paper_id, title, original_message)

def log_not_found(paper_id, title, details):
    """記錄找不到論文的情況。"""
    not_found_info = {'paper_id': paper_id, 'title': title, 'status': 'Not Found', 'details': details}
    s3_key = f"citation_results/not_found/{paper_id}.json"
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(not_found_info, indent=2, ensure_ascii=False))
    print(f"NOT FOUND: Paper ID {paper_id}. Logged to {s3_key}")

def log_permanent_error(paper_id, error_type, error_message, original_message):
    """記錄不可重試的錯誤。"""
    error_info = {
        'error_type': error_type,
        'error_message': error_message,
        'original_message': original_message
    }
    s3_key = f"citation_results/error/{paper_id}_permanent_error.json"
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(error_info, indent=2, ensure_ascii=False))

def handle_retriable_error(e, paper_id, title, original_message):
    """處理可重試的錯誤：記錄詳細資訊並拋出異常以觸發 SQS 重試。"""
    print(f"ERROR processing Paper ID {paper_id}: {type(e).__name__} - {str(e)}")
    error_info = {
        'paper_id': paper_id,
        'title': title,
        'error_type': type(e).__name__,
        'error_message': str(e),
        'full_traceback': traceback.format_exc(),
        'original_message': original_message
    }
    s3_key = f"citation_results/error/{paper_id}_retriable.json"
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(error_info, indent=2, ensure_ascii=False))
    print(f"Detailed retriable error for {paper_id} logged to {s3_key}")
    raise e
