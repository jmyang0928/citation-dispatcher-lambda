import json
import os
import boto3
import pandas as pd
import io

# 從環境變數獲取配置
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
INPUT_CSV_KEY = "consolidated_dataset/arxiv_cleaned_dataset.csv"

# 初始化 AWS 客戶端
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

def handler(event, context):
    """
    Lambda 處理函式。
    讀取 S3 上的 CSV 檔案，並將每一行轉換為一個 SQS 訊息。
    """
    if not SQS_QUEUE_URL or not S3_BUCKET_NAME:
        print("Error: Environment variables SQS_QUEUE_URL or S3_BUCKET_NAME not set.")
        return {'statusCode': 500, 'body': 'Environment variables not set'}

    print(f"Reading paper list from s3://{S3_BUCKET_NAME}/{INPUT_CSV_KEY}...")

    try:
        # 從 S3 讀取 CSV 檔案
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=INPUT_CSV_KEY)
        # 只讀取需要的欄位以節省記憶體
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), usecols=['id', 'title'])
        df.dropna(subset=['id', 'title'], inplace=True)
        
        total_papers = len(df)
        print(f"Found {total_papers} papers to dispatch to SQS...")

        # 使用 SQS SendMessageBatch 分批發送訊息
        batch_size = 10  # SQS SendMessageBatch 的上限為 10
        message_count = 0
        
        for i in range(0, total_papers, batch_size):
            chunk = df.iloc[i:i + batch_size]
            entries = []
            for _, row in chunk.iterrows():
                message_body = {
                    'paper_id': row['id'],
                    'title': str(row['title'])
                }
                # SQS Message ID 只能包含字母、數字和下劃線，長度不超過80個字符
                # 我們使用 paper_id，但替換不允許的字符
                safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', str(row['id']))[:80]
                entries.append({
                    'Id': safe_id,
                    'MessageBody': json.dumps(message_body)
                })
            
            if entries:
                response = sqs.send_message_batch(
                    QueueUrl=SQS_QUEUE_URL,
                    Entries=entries
                )
                if 'Failed' in response and response['Failed']:
                    for failed in response['Failed']:
                        print(f"Failed to send: ID={failed['Id']}, Reason={failed['Message']}")
                
                message_count += len(entries)
                if message_count % 1000 == 0:
                    print(f"Dispatched {message_count}/{total_papers} jobs...")
        
        print(f"Dispatch complete! Total jobs sent to SQS: {message_count}.")
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully dispatched {message_count} jobs.')
        }

    except Exception as e:
        print(f"An error occurred during dispatch: {e}")
        raise e
