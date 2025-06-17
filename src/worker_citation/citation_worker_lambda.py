import json
import os
import boto3
import traceback
import requests
import urllib.parse
import time

# 從環境變數獲取配置
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
OPENALEX_EMAIL = os.environ.get('OPENALEX_EMAIL', 'default-user@example.com')

if not S3_BUCKET_NAME:
    raise ValueError("Environment variable S3_BUCKET_NAME is not set")

# 初始化 S3 客戶端
s3 = boto3.client('s3')

def handler(event, context):
    """
    Lambda 處理函式，由 SQS 觸發。
    此函式預期每個 SQS record 的 body 是一個包含多篇論文的 JSON 列表。
    新增了檢查 S3 是否已存在結果的功能。
    """
    for record in event.get('Records', []):
        original_message = record.get('body', '[]')
        
        try:
            paper_batch = json.loads(original_message)
            if not isinstance(paper_batch, list):
                raise TypeError("Message body is not a list.")
                
            print(f"收到一個包含 {len(paper_batch)} 篇論文的批次，開始處理...")

            # 迴圈處理批次中的每一篇論文
            for paper_data in paper_batch:
                paper_id = paper_data.get('id')
                title = paper_data.get('title')

                if not paper_id or not title:
                    print(f"批次中發現無效的論文紀錄: {paper_data}")
                    log_permanent_error(paper_id or 'unknown_id', 'MalformedRecordInBatch', 'Record in batch is missing paper_id or title', str(paper_data))
                    continue
                
                # --- 主要邏輯修改點：檢查結果是否已存在 ---
                success_key = f"citation_results/success/{paper_id}.json"
                try:
                    s3.head_object(Bucket=S3_BUCKET_NAME, Key=success_key)
                    print(f"  - 結果已存在，跳過 Paper ID: {paper_id}")
                    continue # 如果檔案存在，直接跳到下一篇論文
                except s3.exceptions.ClientError as e:
                    # 如果錯誤是 404 (Not Found)，表示檔案不存在，這是我們預期的情況
                    if e.response['Error']['Code'] == '404':
                        pass # 檔案不存在，繼續執行下面的處理邏輯
                    else:
                        # 如果是其他 ClientError (如權限問題)，則記錄並跳過
                        print(f"  - 檢查 S3 物件時發生非 404 錯誤: {e}")
                        log_permanent_error(paper_id, 'S3HeadObjectError', str(e), str(paper_data))
                        continue
                
                print(f"  - 正在處理 Paper ID: {paper_id}")
                process_paper(paper_id, title, str(paper_data))
                time.sleep(0.11)

        except (json.JSONDecodeError, TypeError) as e:
            print(f"無法解析批次訊息或格式錯誤: {e}")
            log_permanent_error('unknown_batch_id', type(e).__name__, str(e), original_message)
            continue

        except Exception as e:
            print(f"批次處理中發生可重試的錯誤，將重試整個批次。錯誤: {e}")
            raise e

def process_paper(paper_id: str, title: str, original_record_str: str):
    """
    使用 OpenAlex API 處理單篇論文的查詢邏輯。
    """
    try:
        safe_title = urllib.parse.quote_plus(f'"{title}"')
        works_url = f"https://api.openalex.org/works?search={safe_title}&per_page=1&mailto={OPENALEX_EMAIL}"
        
        r_works = requests.get(works_url, timeout=30)
        r_works.raise_for_status()
        works_data = r_works.json()

        if not works_data.get("results"):
            log_not_found(paper_id, title, 'No results returned from OpenAlex.')
            return

        paper_data = works_data["results"][0]

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

        result = {
            'original_id': paper_id,
            'searched_title': title,
            'found_paper_title': paper_data.get('display_name'),
            'citationCount': paper_data.get('cited_by_count'),
            'authors': author_info
        }
        
        s3_key = f"citation_results/success/{paper_id}.json"
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(result, indent=2, ensure_ascii=False))
        print(f"  -> SUCCESS: Paper ID {paper_id} processed and saved.")

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        if status_code == 429 or status_code >= 500:
            handle_retriable_error(e, paper_id, title, original_record_str)
        elif status_code == 404:
            log_not_found(paper_id, title, 'OpenAlex API returned HTTP 404.')
        else:
            log_permanent_error(paper_id, f"HTTPError_{status_code}", str(e), original_record_str)

    except Exception as e:
        handle_retriable_error(e, paper_id, title, original_record_str)

def log_not_found(paper_id, title, details):
    not_found_info = {'paper_id': paper_id, 'title': title, 'status': 'Not Found', 'details': details}
    s3_key = f"citation_results/not_found/{paper_id}.json"
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(not_found_info, indent=2, ensure_ascii=False))
    print(f"  -> NOT FOUND: Paper ID {paper_id} logged.")

def log_permanent_error(paper_id, error_type, error_message, original_message):
    error_info = {'error_type': error_type, 'error_message': error_message, 'original_message': original_message}
    s3_key = f"citation_results/error/{paper_id}_permanent_error.json"
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(error_info, indent=2, ensure_ascii=False))

def handle_retriable_error(e, paper_id, title, original_message):
    print(f"  -> ERROR processing Paper ID {paper_id}: {type(e).__name__} - {str(e)}")
    error_info = {
        'paper_id': paper_id, 'title': title, 'error_type': type(e).__name__,
        'error_message': str(e), 'full_traceback': traceback.format_exc(),
        'original_message': original_message
    }
    s3_key = f"citation_results/error/{paper_id}_retriable.json"
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(error_info, indent=2, ensure_ascii=False))
    raise e
