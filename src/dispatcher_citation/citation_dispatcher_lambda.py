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
BATCH_SIZE = 100 # 設定每個 SQS 訊息中包含的論文數量

# 初始化 AWS 客戶端
s3 = boto3.client('s3')
sqs = boto3.client('sqs')
lambda_client = boto3.client('lambda')

def handler(event, context):
    """
    掃描 S3 的 .jsonl 檔案，將論文打包成大小為 BATCH_SIZE 的批次，
    並將每個批次作為單一訊息發送到 SQS。
    具備接續執行能力以避免超時。
    """
    if not SQS_QUEUE_URL or not S3_BUCKET_NAME:
        print("錯誤：環境變數 SQS_QUEUE_URL 或 S3_BUCKET_NAME 未設定。")
        return {'statusCode': 500, 'body': '環境變數未設定'}

    start_index = event.get('start_index', 0)
    all_files = event.get('file_list')

    if all_files is None:
        print("首次執行：正在獲取並排序所有檔案列表...")
        all_files = []
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=INPUT_S3_PREFIX)
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.jsonl'):
                        all_files.append(obj['Key'])
        all_files.sort(reverse=True)
        print(f"找到 {len(all_files)} 個檔案。將從最新的月份開始處理。")

    files_to_process = all_files[start_index:]
    if not files_to_process:
        print("沒有更多檔案需要處理。流程完成。")
        return {'statusCode': 200, 'body': '所有檔案都已分派。'}
    
    total_batches_dispatched = 0

    try:
        paper_buffer = [] # 用於存放跨檔案的論文批次
        
        for idx, file_key in enumerate(files_to_process):
            current_file_index = start_index + idx
            
            if context.get_remaining_time_in_millis() < 30000:
                print("時間即將耗盡，觸發下一個 Lambda 接續執行。")
                next_start_index = current_file_index
                new_payload = {'start_index': next_start_index, 'file_list': all_files}
                
                lambda_client.invoke(
                    FunctionName=context.function_name,
                    InvocationType='Event',
                    Payload=json.dumps(new_payload)
                )
                print(f"已成功觸發下一個函式，將從檔案索引 {next_start_index} 開始。")
                return {'statusCode': 200, 'body': '待下一次執行接續...'}

            print(f"正在處理檔案 #{current_file_index + 1}/{len(all_files)}: {file_key}...")
            
            file_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            df = pd.read_json(io.BytesIO(file_obj['Body'].read()), lines=True)
            
            if 'id' in df.columns and 'title' in df.columns:
                df_subset = df[['id', 'title']].dropna()
                records = df_subset.to_dict('records')
                paper_buffer.extend(records)

                # 當緩衝區中的論文數量達到 BATCH_SIZE 時，發送到 SQS
                while len(paper_buffer) >= BATCH_SIZE:
                    batch_to_send = paper_buffer[:BATCH_SIZE]
                    paper_buffer = paper_buffer[BATCH_SIZE:]
                    send_batch_to_sqs(batch_to_send)
                    total_batches_dispatched += 1
            else:
                print(f"  [警告] 檔案 {file_key} 缺少 'id' 或 'title' 欄位，已跳過。")

        # 處理迴圈結束後剩餘在緩衝區中的論文
        if paper_buffer:
            send_batch_to_sqs(paper_buffer)
            total_batches_dispatched += 1
            
        print("\n--- 分派完成 ---")
        print(f"本次執行鏈總共處理了 {len(files_to_process)} 個檔案。")
        print(f"總共發送了 {total_batches_dispatched} 個批次 (每個批次約 {BATCH_SIZE} 篇論文) 到 SQS。")
        
        return {'statusCode': 200, 'body': f'成功分派了 {total_batches_dispatched} 個批次。'}

    except Exception as e:
        print(f"分派過程中發生錯誤: {e}")
        raise e

def send_batch_to_sqs(paper_batch: list):
    """將一個論文批次作為單一 SQS 訊息發送。"""
    if not paper_batch:
        return
    
    # 使用批次中第一篇論文的 ID 作為大致的識別符
    batch_id = paper_batch[0].get('id', 'batch')
    safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', str(batch_id))[:80]

    sqs.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(paper_batch)
    )
    print(f"  -> 已分派一個包含 {len(paper_batch)} 篇論文的批次。")

