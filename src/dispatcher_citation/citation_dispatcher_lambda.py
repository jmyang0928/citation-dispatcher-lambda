import json, os, boto3, pandas as pd, io, re

SQS_QUEUE_URL  = os.environ['SQS_QUEUE_URL']
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
INPUT_CSV_KEY  = "consolidated_dataset/arxiv_cleaned_dataset.csv"

s3  = boto3.client('s3')
sqs = boto3.client('sqs')

def handler(event, context):
    print(f"Reading paper list from s3://{S3_BUCKET_NAME}/{INPUT_CSV_KEY} ...")

    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=INPUT_CSV_KEY)
    # 用 chunksize ＝ 5 000 列（~5–10 MB）逐塊讀
    for chunk in pd.read_csv(
        io.BytesIO(obj['Body'].read()),
        usecols=['id', 'title'],
        chunksize=5_000,
    ):
        entries = []
        for _, row in chunk.iterrows():
            safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', str(row["id"]))[:80]
            entries.append({
                "Id": safe_id,
                "MessageBody": json.dumps({
                    "paper_id": row["id"],
                    "title": str(row["title"]),
                }),
            })
            # SQS SendMessageBatch 最多 10 筆；湊滿就送一次
            if len(entries) == 10:
                sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
                entries.clear()

        # 處理 chunk 結尾不足 10 筆的殘餘
        if entries:
            sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)

    print("Dispatch complete!")
