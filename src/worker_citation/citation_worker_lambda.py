import json
import os
import boto3
from semanticscholar import SemanticScholar, PaperNotFoundException

# 從環境變數獲取 S3 儲存桶名稱
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
if not S3_BUCKET_NAME:
    raise ValueError("Environment variable S3_BUCKET_NAME is not set")

# 初始化客戶端
s3 = boto3.client('s3')
# 在 Lambda 全局範圍內初始化，以便在 warm start 時重複使用連線
# timeout 應小於 Lambda 的總超時時間
sch = SemanticScholar(timeout=30)

def handler(event, context):
    """
    Lambda 處理函式，由 SQS 觸發。
    """
    for record in event.get('Records', []):
        paper_id = 'unknown_id'
        try:
            message_body = json.loads(record.get('body', '{}'))
            paper_id = message_body.get('paper_id')
            title = message_body.get('title')

            if not paper_id or not title:
                print(f"Invalid message, missing paper_id or title: {record.get('body')}")
                # 不需要重試這種格式錯誤的訊息
                continue

            print(f"Processing Paper ID: {paper_id}")
            
            # 執行 Semantic Scholar 查詢
            process_paper(paper_id, title)

        except Exception as e:
            # 捕獲所有未預期的錯誤，例如 JSON 解析錯誤或未知異常
            error_info = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'original_message': record.get('body')
            }
            s3_key = f"citation_results/error/{paper_id}.json"
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(error_info, indent=2, ensure_ascii=False)
            )
            print(f"An unexpected error occurred for Paper ID {paper_id}. Details logged to {s3_key}")
            # 不拋出異常，讓 SQS 認為訊息已處理，避免因程式碼 Bug 導致無限重試


def process_paper(paper_id: str, title: str):
    """
    處理單篇論文的查詢邏輯，並將結果寫入 S3。
    """
    try:
        # 1. 搜尋論文
        search_results = sch.search_paper(
            query=title, 
            limit=1, 
            fields=["title", "citationCount", "authors.name", "authors.authorId"]
        )

        if not search_results or not search_results[0].get('paperId'):
            raise PaperNotFoundException(f"No results found for title '{title}'.")

        paper = search_results[0]
        
        # 2. 獲取作者的 H-index
        author_ids = [a['authorId'] for a in paper.get('authors', []) if a.get('authorId')]
        authors_details = []
        if author_ids:
            batch_size = 100 # API 一次最多接受 500 個 ID，但設小一點更安全
            for i in range(0, len(author_ids), batch_size):
                batch_ids = author_ids[i:i+batch_size]
                authors_details.extend(sch.get_authors(batch_ids, fields=["name", "hIndex"]))

        author_info = [{'name': a.name, 'hIndex': a.hIndex} for a in authors_details if a]

        # 3. 組合成功結果
        result = {
            'original_id': paper_id,
            'searched_title': title,
            'found_paper_title': paper.get('title'),
            'citationCount': paper.get('citationCount'),
            'authors': author_info
        }
        
        s3_key = f"citation_results/success/{paper_id}.json"
        s3.put_object(
            Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(result, indent=2, ensure_ascii=False)
        )
        print(f"SUCCESS: Paper ID {paper_id} processed and saved to {s3_key}")

    except PaperNotFoundException as e:
        # 處理找不到論文的情況 (404)
        not_found_info = {'paper_id': paper_id, 'title': title, 'status': 'Not Found', 'details': str(e)}
        s3_key = f"citation_results/not_found/{paper_id}.json"
        s3.put_object(
            Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(not_found_info, indent=2, ensure_ascii=False)
        )
        print(f"NOT FOUND: Paper ID {paper_id}. Logged to {s3_key}")
    
    except Exception as e:
        # 處理 API 請求失敗或其他可重試的錯誤
        print(f"ERROR processing Paper ID {paper_id}: {type(e).__name__} - {str(e)}")
        # 拋出異常，這樣 SQS 會在 Visibility Timeout 後讓訊息重新可見，從而觸發重試
        raise e

