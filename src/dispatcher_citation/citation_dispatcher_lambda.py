import json
import boto3
import os

# --- Environment Variables ---
BUCKET = os.environ['BUCKET']
INPUT_KEY = os.environ['INPUT_KEY']
BATCH_PREFIX = os.environ.get('BATCH_PREFIX', 'citation_batches_tmp')
PROCESS_QUEUE_URL = os.environ['PROCESS_QUEUE_URL']
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 100))

# --- Boto3 Clients ---
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

def create_batch_file(lines, batch_number):
    """
    Writes a list of lines to a batch file on S3 and returns the SQS message payload.
    """
    if not lines:
        return None
        
    batch_key = f"{BATCH_PREFIX}/batch_{batch_number}.jsonl"
    batch_content = "\n".join(lines)
    
    # 1. Write the batch file to S3
    s3_client.put_object(
        Bucket=BUCKET,
        Key=batch_key,
        Body=batch_content
    )
    print(f"Successfully wrote batch file: s3://{BUCKET}/{batch_key}")

    # 2. Prepare the message payload for SQS
    sqs_message = {
        "bucket": BUCKET,
        "key": batch_key
    }
    
    return sqs_message

def lambda_handler(event, context):
    """
    This dispatcher reads a large file, splits it into smaller files on S3,
    and sends messages for each smaller file to SQS in REVERSE order.
    """
    print(f"Starting dispatcher for s3://{BUCKET}/{INPUT_KEY}")

    s3_object = s3_client.get_object(Bucket=BUCKET, Key=INPUT_KEY)
    s3_object_body = s3_object['Body']

    all_sqs_messages = []
    current_batch_lines = []
    batch_counter = 0
    lines_processed = 0

    # Step 1: Stream the input file and create all batch files first
    for line_bytes in s3_object_body.iter_lines():
        line = line_bytes.decode('utf-8')
        if not line:
            continue
            
        current_batch_lines.append(line)
        lines_processed += 1
        
        if len(current_batch_lines) >= BATCH_SIZE:
            message_payload = create_batch_file(current_batch_lines, batch_counter)
            all_sqs_messages.append(message_payload)
            batch_counter += 1
            current_batch_lines = []

    # Handle the final, potentially smaller, batch
    if current_batch_lines:
        message_payload = create_batch_file(current_batch_lines, batch_counter)
        all_sqs_messages.append(message_payload)

    print(f"Created {len(all_sqs_messages)} total batch files. Now sending to SQS in reverse order.")

    # Step 2: Send all collected messages to SQS in reverse order
    for message in reversed(all_sqs_messages):
        sqs_client.send_message(
            QueueUrl=PROCESS_QUEUE_URL,
            MessageBody=json.dumps(message)
        )
    
    print("All messages sent to SQS.")
    
    summary = f"Dispatch complete. Processed {lines_processed} lines into {len(all_sqs_messages)} batches and sent to SQS in reverse."
    
    return {
        'statusCode': 200,
        'body': json.dumps(summary)
    }