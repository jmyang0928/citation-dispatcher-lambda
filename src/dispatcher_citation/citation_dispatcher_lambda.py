import json
import os
import boto3
import pandas as pd
import io
import re

# From environment variables
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
INPUT_S3_PREFIX = os.environ.get('INPUT_S3_PREFIX', 'cleaned_data/')

# AWS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')
lambda_client = boto3.client('lambda')

def handler(event, context):
    """
    Scans all .jsonl files in a given S3 prefix and dispatches each line as a message to SQS.
    This function is resumable and will invoke itself to continue processing if it's about to time out.
    """
    if not SQS_QUEUE_URL or not S3_BUCKET_NAME:
        print("Error: Environment variables SQS_QUEUE_URL or S3_BUCKET_NAME not set.")
        return {'statusCode': 500, 'body': 'Environment variables not set'}

    paginator = s3.get_paginator('list_objects_v2')
    
    # Check for a continuation token from a previous run
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
            # Check remaining time; if less than 30 seconds, reinvoke and exit.
            if context.get_remaining_time_in_millis() < 30000:
                print("Time is running out. Invoking next lambda to continue.")
                next_token = page.get('NextContinuationToken')
                if next_token:
                    new_payload = {'continuation_token': next_token}
                    lambda_client.invoke(
                        FunctionName=context.function_name,
                        InvocationType='Event',
                        Payload=json.dumps(new_payload)
                    )
                    print(f"Successfully invoked next function with token: {next_token}")
                    return {'statusCode': 200, 'body': 'To be continued in next invocation...'}
                else:
                    print("Last page processed just in time. No more files to process.")
                    break

            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                file_key = obj['Key']
                if not file_key.endswith('.jsonl'):
                    continue
                
                total_files_processed += 1
                print(f"Processing file #{total_files_processed}: {file_key}...")
                
                file_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
                
                # --- FIX IS HERE ---
                # First, load the data from the JSON lines file
                df_full = pd.read_json(io.BytesIO(file_obj['Body'].read()), lines=True)
                
                # Then, select the columns you need, ensuring they exist
                if 'id' in df_full.columns and 'title' in df_full.columns:
                    df = df_full[['id', 'title']].dropna()
                else:
                    print(f"  [Warning] File {file_key} is missing 'id' or 'title' columns. Skipping.")
                    df = pd.DataFrame() # create empty df to skip dispatch

                # Dispatch all tasks from the current file
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
    """Dispatches each row of a DataFrame to SQS."""
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
            response = sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
            if 'Failed' in response and response['Failed']:
                for failed in response['Failed']:
                    print(f"  [Warning] Failed to send message: ID={failed['Id']}, Reason={failed['Message']}")
            message_count += len(entries)
            
    print(f"  -> Dispatched {message_count} jobs from this file.")
    return message_count
