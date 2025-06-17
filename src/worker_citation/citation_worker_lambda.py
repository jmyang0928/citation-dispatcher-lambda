import json
import os
import boto3
from semanticscholar import SemanticScholar
# 我們不再需要導入特定的異常，因為我們將改變處理邏輯

# 從環境變數獲取 S3 儲存桶名稱
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
if not S3_BUCKET_NAME:
    raise ValueError("Environment variable S3_BUCKET_NAME is not set")

# 初始化客戶端
s3 = boto3.client('s3')
# 在 Lambda 全局範圍內初始化，以便在 warm start 時重複使用連線
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
                continue

            print(f"Processing Paper ID: {paper_id}")
            
            # 執行核心查詢邏輯
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

        # --- 主要邏輯變更點 ---
        # 如果找不到論文，直接處理 "Not Found" 情況並返回
        if not search_results or not search_results[0].get('paperId'):
            not_found_info = {'paper_id': paper_id, 'title': title, 'status': 'Not Found', 'details': f"No results found for title '{title}'."}
            s3_key = f"citation_results/not_found/{paper_id}.json"
            s3.put_object(
                Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(not_found_info, indent=2, ensure_ascii=False)
            )
            print(f"NOT FOUND: Paper ID {paper_id}. Logged to {s3_key}")
            return # 直接結束此函式

        # 如果找到了論文，繼續執行
        paper = search_results[0]
        
        # 2. 獲取作者的 H-index
        author_ids = [a['authorId'] for a in paper.get('authors', []) if a.get('authorId')]
        authors_details = []
        if author_ids:
            batch_size = 100
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
    
    except Exception as e:
        # 這個 except 區塊現在只會捕獲真正的 API 錯誤或網路問題
        print(f"ERROR processing Paper ID {paper_id}: {type(e).__name__} - {str(e)}")
        # 拋出異常，這樣 SQS 會在 Visibility Timeout 後讓訊息重新可見，從而觸發重試
        raise e
