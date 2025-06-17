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
    掃描 S3 上指定前綴的所有 .jsonl 檔案，從最新的月份開始，
    並將每一行轉換為一個 SQS 訊息。
    - 如果執行時間即將耗盡，此函式會觸發自己以接續執行。
    - 支援測試模式，僅發送一筆資料。
    """
    if not SQS_QUEUE_URL or not S3_BUCKET_NAME:
        print("錯誤：環境變數 SQS_QUEUE_URL 或 S3_BUCKET_NAME 未設定。")
        return {'statusCode': 500, 'body': '環境變數未設定'}

    # --- 測試模式邏輯 ---
    if event.get('test_mode') is True:
        return run_test_mode()

    # --- 常規執行邏輯 ---
    
    # 檢查是否有來自上一次執行的接續狀態
    start_index = event.get('start_index', 0)
    all_files = event.get('file_list')

    # 如果這是首次執行，獲取並排序完整的檔案列表
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
        # 反向排序，確保從最新的月份 (例如 'papers_2405.jsonl') 開始處理
        all_files.sort(reverse=True)
        print(f"找到 {len(all_files)} 個檔案。將從最新的月份開始處理。")

    # 決定本次執行要處理的檔案
    files_to_process = all_files[start_index:]
    if not files_to_process:
        print("沒有更多檔案需要處理。流程完成。")
        return {'statusCode': 200, 'body': '所有檔案都已分派。'}
    
    total_jobs_dispatched = 0

    try:
        for idx, file_key in enumerate(files_to_process):
            current_file_index = start_index + idx
            
            # 檢查剩餘執行時間
            if context.get_remaining_time_in_millis() < 30000:
                print("時間即將耗盡，觸發下一個 Lambda 接續執行。")
                next_start_index = current_file_index # 下次從當前檔案開始
                new_payload = {'start_index': next_start_index, 'file_list': all_files}
                
                lambda_client.invoke(
                    FunctionName=context.function_name,
                    InvocationType='Event', # 非同步觸發
                    Payload=json.dumps(new_payload)
                )
                print(f"已成功觸發下一個函式，將從檔案索引 {next_start_index} 開始。")
                return {'statusCode': 200, 'body': '待下一次執行接續...'}

            print(f"正在處理檔案 #{current_file_index + 1}/{len(all_files)}: {file_key}...")
            
            file_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            df = pd.read_json(io.BytesIO(file_obj['Body'].read()), lines=True)
            
            if 'id' in df.columns and 'title' in df.columns:
                df_subset = df[['id', 'title']].dropna()
                dispatched_in_file = dispatch_dataframe(df_subset)
                total_jobs_dispatched += dispatched_in_file
            else:
                print(f"  [警告] 檔案 {file_key} 缺少 'id' 或 'title' 欄位，已跳過。")

        print("\n--- 分派完成 ---")
        print(f"本次執行鏈總共處理了 {len(files_to_process)} 個檔案。")
        print(f"總共發送了 {total_jobs_dispatched} 個任務到 SQS。")
        
        return {'statusCode': 200, 'body': f'成功從 {len(files_to_process)} 個檔案中分派了 {total_jobs_dispatched} 個任務。'}

    except Exception as e:
        print(f"分派過程中發生錯誤: {e}")
        raise e

def run_test_mode():
    """執行測試模式，僅分派一筆資料。"""
    print("🚀 --- 執行測試模式 --- 🚀")
    paginator = s3.get_paginator('list_objects_v2')
    # 獲取任何一個 .jsonl 檔案
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=INPUT_S3_PREFIX)
    first_file_key = None
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.jsonl'):
                    first_file_key = obj['Key']
                    break
            if first_file_key:
                break
    
    if first_file_key:
        print(f"找到測試檔案: {first_file_key}")
        file_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=first_file_key)
        # 讀取第一行
        first_line = file_obj['Body'].read().splitlines()[0].decode('utf-8')
        record = json.loads(first_line)
        df = pd.DataFrame([record])
        if 'id' in df.columns and 'title' in df.columns:
            dispatch_dataframe(df[['id', 'title']])
            print("✅ 測試模式完成，已發送一筆資料。")
            return {'statusCode': 200, 'body': '測試模式成功完成。'}
        else:
            return {'statusCode': 400, 'body': '測試檔案的第一筆資料缺少 id 或 title。'}
    else:
        print("找不到任何檔案進行測試。")
        return {'statusCode': 404, 'body': '找不到任何檔案進行測試。'}

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
    print(f"  -> 已從此檔案分派 {message_count} 個任務。")
    return message_count
