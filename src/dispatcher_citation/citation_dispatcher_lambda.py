import os
import json
import boto3
import logging
from typing import Dict, Any, List

# --- Configuration ---
# Lambda 環境變數
BUCKET = os.environ['BUCKET']
INPUT_KEY = os.environ['INPUT_KEY']
BATCH_PREFIX = os.environ['BATCH_PREFIX']
PROCESS_QUEUE_URL = os.environ['PROCESS_QUEUE_URL']
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 100))

# Boto3 Clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event: Dict[str, Any], context: object) -> Dict[str, Any]:
    """
    Reads a large JSONL from S3, splits it into smaller batches, uploads them,
    and then sends SQS messages in REVERSE order of the batches.
    """
    logger.info(
        f"Starting reverse dispatcher for s3://{BUCKET}/{INPUT_KEY} with batch size {BATCH_SIZE}"
    )

    try:
        s3_object = s3_client.get_object(Bucket=BUCKET, Key=INPUT_KEY)
        body_stream = s3_object['Body']

        batch = []
        batch_id = 0
        total_lines = 0
        
        # 用於暫存所有待發送的 SQS 訊息
        messages_to_send = []

        # --- 第一階段：讀取、切分、上傳批次檔 ---
        logger.info("Phase 1: Reading input file and uploading batches to S3...")
        for line in body_stream.iter_lines():
            if not line:
                continue

            batch.append(line.decode('utf-8'))
            total_lines += 1

            if len(batch) >= BATCH_SIZE:
                # 處理並收集訊息，但不立即發送
                message = process_and_stage_batch(batch, batch_id)
                messages_to_send.append(message)
                batch_id += 1
                batch = []

        if batch:
            message = process_and_stage_batch(batch, batch_id)
            messages_to_send.append(message)

        logger.info(f"Phase 1 complete. Processed {total_lines} lines into {len(messages_to_send)} batches.")

        # --- 第二階段：反序並批量發送 SQS 訊息 ---
        logger.info("Phase 2: Sending SQS messages in reverse order...")
        
        # 反轉訊息列表
        messages_to_send.reverse()
        
        sent_count = 0
        # SQS send_message_batch 每次最多 10 則
        for i in range(0, len(messages_to_send), 10):
            chunk = messages_to_send[i:i + 10]
            
            sqs_entries = []
            for idx, msg_body in enumerate(chunk):
                sqs_entries.append({
                    'Id': f'batch_msg_{i+idx}', # Batch-Request-Entry 的唯一標識符
                    'MessageBody': json.dumps(msg_body)
                })

            response = sqs_client.send_message_batch(
                QueueUrl=PROCESS_QUEUE_URL,
                Entries=sqs_entries
            )
            
            # 檢查是否有發送失敗的訊息
            if 'Failed' in response and response['Failed']:
                for failed_entry in response['Failed']:
                    logger.error(f"Failed to send message ID {failed_entry['Id']}: {failed_entry['Message']}")
            
            sent_count += len(chunk)
            
        logger.info(f"Phase 2 complete. Successfully sent {sent_count} messages to SQS.")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Reverse dispatch completed successfully!',
                'total_records': total_lines,
                'total_batches_created': len(messages_to_send),
                'messages_sent_to_sqs': sent_count
            })
        }

    except Exception as e:
        logger.error(f"An error occurred during dispatch: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def process_and_stage_batch(batch: list, batch_id: int) -> Dict[str, Any]:
    """
    Uploads a batch to S3 and returns the message payload to be staged.
    """
    batch_content = "\n".join(batch)
    batch_key = f"{BATCH_PREFIX}input_batch_{batch_id}.jsonl"

    # 1. 上傳批次檔到 S3
    s3_client.put_object(
        Bucket=BUCKET,
        Key=batch_key,
        Body=batch_content.encode('utf-8')
    )
    logger.info(f"Staged batch {batch_id} to s3://{BUCKET}/{batch_key}")

    # 2. 準備 SQS 訊息內容並返回
    message_body = {
        "batch_id": batch_id,
        "s3_key": batch_key
    }
    return message_body