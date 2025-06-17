import json
import os
import boto3
import pandas as pd
import io
import re

# 從環境變數獲取配置
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
INPUT_S3_PREFIX = os.environ.get('INPUT_S3_PREFIX', 'cleaned_data/')

# 初始化 AWS 客戶端
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

def handler(event, context):
    """
    Lambda 處理函式。
    掃描 S3 上指定前綴的所有 .jsonl 檔案，並將每一行轉換為一個 SQS 訊息。
    """
    if not SQS_QUEUE_URL or not S3_BUCKET_NAME:
        print("Error: Environment variables SQS_QUEUE_URL or S3_BUCKET_NAME not set.")
        return {'statusCode': 500, 'body': 'Environment variables not set'}

    print(f"Scanning for .jsonl files in s3://{S3_BUCKET_NAME}/{INPUT_S3_PREFIX}...")

    try:
        # 使用 paginator 處理大量檔案
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=INPUT_S3_PREFIX)

        total_jobs_dispatched = 0
        total_files_processed = 0

        for page in pages:
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                file_key = obj['Key']
                if not file_key.endswith('.jsonl'):
                    continue
                
                print(f"Processing file: {file_key}...")
                total_files_processed += 1
                
                # 讀取單個 .jsonl 檔案
                file_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
                
                # 使用 read_json(lines=True) 高效讀取 jsonl
                df = pd.read_json(io.BytesIO(file_obj['Body'].read()), lines=True)
                df = df[['id', 'title']].dropna()

                # 分派該檔案中的所有任務
                dispatched_in_file = dispatch_dataframe(df)
                total_jobs_dispatched += dispatched_in_file
        
        print("\n--- Dispatch Complete ---")
        print(f"Processed {total_files_processed} files.")
        print(f"Total jobs sent to SQS: {total_jobs_dispatched}.")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully dispatched {total_jobs_dispatched} jobs from {total_files_processed} files.')
        }

    except Exception as e:
        print(f"An error occurred during dispatch: {e}")
        raise e

def dispatch_dataframe(df: pd.DataFrame) -> int:
    """將 DataFrame 中的每一行分派到 SQS。"""
    if df.empty:
        return 0

    batch_size = 10  # SQS SendMessageBatch 的上限
    message_count = 0
    
    for i in range(0, len(df), batch_size):
        chunk = df.iloc[i:i + batch_size]
        entries = []
        for _, row in chunk.iterrows():
            message_body = {'paper_id': row['id'], 'title': str(row['title'])}
            safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', str(row['id']))[:80]
            entries.append({'Id': safe_id, 'MessageBody': json.dumps(message_body)})
        
        if entries:
            response = sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
            if 'Failed' in response and response['Failed']:
                for failed in response['Failed']:
                    print(f"  [Warning] Failed to send message: ID={failed['Id']}, Reason={failed['Message']}")
            message_count += len(entries)
            
    print(f"  -> Dispatched {message_count} jobs from this file.")
    return message_count
