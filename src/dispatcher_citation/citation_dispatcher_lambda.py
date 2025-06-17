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
lambda_client = boto3.client('lambda')

def handler(event, context):
    """
    Lambda 處理函式。
    掃描 S3 上指定前綴的所有 .jsonl 檔案，並將每一行轉換為一個 SQS 訊息。
    如果執行時間即將耗盡，此函式會觸發自己以接續執行。
    """
    if not SQS_QUEUE_URL or not S3_BUCKET_NAME:
        print("Error: Environment variables SQS_QUEUE_URL or S3_BUCKET_NAME not set.")
        return {'statusCode': 500, 'body': 'Environment variables not set'}

    # 使用 paginator 處理 S3 中的大量物件
    paginator = s3.get_paginator('list_objects_v2')
    
    # 檢查是否有來自上一次執行的接續權杖 (continuation token)
    pagination_config = {}
    continuation_token = event.get('continuation_token')
    if continuation_token:
        print(f"Resuming dispatch from token: {continuation_token}")
        pagination_config['StartingToken'] = continuation_token

    pages = paginator.paginate(
        Bucket=S3_BUCKET_NAME, 
        Prefix=INPUT_S3_PREFIX,
        PaginationConfig=pagination_config
    )

    total_jobs_dispatched = 0
    total_files_processed = 0

    try:
        for page in pages:
            # 檢查剩餘執行時間
            # 如果剩餘時間少於 30 秒，則觸發下一個 Lambda 並退出
            if context.get_remaining_time_in_millis() < 30000:
                print("Time is running out. Invoking next lambda to continue.")
                
                # 'NextContinuationToken' 只有在頁面被截斷時才存在
                next_token = page.get('NextContinuationToken')
                if next_token:
                    new_payload = {'continuation_token': next_token}
                    lambda_client.invoke(
                        FunctionName=context.function_name,
                        InvocationType='Event', # 非同步觸發
                        Payload=json.dumps(new_payload)
                    )
                    print(f"Successfully invoked next function with token: {next_token}")
                    return {'statusCode': 200, 'body': 'To be continued in next invocation...'}
                else:
                    # 這是最後一頁，且時間不足，但我們已處理完畢
                    print("Last page processed just in time. No more files to process.")
                    break # 結束迴圈

            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                file_key = obj['Key']
                if not file_key.endswith('.jsonl'):
                    continue
                
                total_files_processed += 1
                print(f"Processing file #{total_files_processed}: {file_key}...")
                
                file_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
                df = pd.read_json(io.BytesIO(file_obj['Body'].read()), lines=True, usecols=['id', 'title']).dropna()

                dispatched_in_file = dispatch_dataframe(df)
                total_jobs_dispatched += dispatched_in_file
        
        print("\n--- Dispatch Complete ---")
        print(f"Processed {total_files_processed} files in this invocation chain.")
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

    batch_size = 10
    message_count = 0
    
    for i in range(0, len(df), batch_size):
        chunk = df.iloc[i:i + batch_size]
        entries = []
        for _, row in chunk.iterrows():
            message_body = {'paper_id': row['id'], 'title': str(row['title'])}
            safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', str(row['id']))[:80]
            entries.append({'Id': safe_id, 'MessageBody': json.dumps(message_body)})
        
        if entries:
            sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
            message_count += len(entries)
            
    print(f"  -> Dispatched {message_count} jobs from this file.")
    return message_count

