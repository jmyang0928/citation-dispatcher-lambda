import json
import requests
import urllib.parse
import time
import os
import boto3
from concurrent.futures import ThreadPoolExecutor
import secrets

# --- Environment Variables ---
BUCKET = os.environ['BUCKET']
OPENALEX_EMAIL = os.environ['OPENALEX_EMAIL']
SUCCESS_PREFIX = os.environ.get('SUCCESS_PREFIX', 'results/success')
FAILURE_PREFIX = os.environ.get('FAILURE_PREFIX', 'results/failure')

# --- Boto3/API Clients ---
s3_client = boto3.client('s3')
OPENALEX_BASE = "https://api.openalex.org"

# get_work_and_authors & get_author_hindex functions remain the same
def get_work_and_authors(title: str, email: str):
    if not title:
        raise ValueError("Paper title cannot be empty.")
    quoted_title = urllib.parse.quote_plus(f'"{title}"')
    url = f"{OPENALEX_BASE}/works?search={quoted_title}&per_page=1&mailto={email}"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        results = resp.json().get('results', [])
        if not results: return None, 0, []
        item = results[0]
        return item.get('display_name'), item.get('cited_by_count', 0), item.get('authorships', [])
    except requests.exceptions.RequestException as e:
        raise e

def get_author_hindex(author_id: str, email: str):
    if not author_id: return "Unknown Author", 0
    author_short_id = author_id.split('/')[-1]
    url = f"{OPENALEX_BASE}/authors/{author_short_id}?mailto={email}"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get('display_name', 'N/A'), data.get('summary_stats', {}).get('h_index', 0)
    except requests.exceptions.RequestException as e:
        raise e

def process_single_paper(paper_info: dict):
    arxiv_id = paper_info.get('id')
    title = paper_info.get('title')

    if not arxiv_id or not title:
        print(f"Skipping record due to missing data: {paper_info}")
        return

    try:
        work_title, citations, authorships = get_work_and_authors(title, secrets.token_hex(4) + "@example.com")
        if not work_title:
            raise ValueError("Work not found via OpenAlex")

        authors_info = []
        for a in authorships:
            author_data = a.get('author', {})
            author_id = author_data.get('id')
            time.sleep(0.1)  # Rate limiting
            name, h_index = get_author_hindex(author_id, secrets.token_hex(4) + "@example.com")
            authors_info.append({"name": name, "h_index": h_index, "openalex_id": author_id})

        success_payload = {
            "arxiv_id": arxiv_id, "query_title": title, "found_work_title": work_title,
            "citation_count": citations, "authors": authors_info
        }
        s3_key = f"{SUCCESS_PREFIX}/{arxiv_id}.jsonl"
        s3_client.put_object(Bucket=BUCKET, Key=s3_key, Body=json.dumps(success_payload))

    except Exception as e:
        failure_payload = {"input_paper_info": paper_info, "error_message": str(e)}
        s3_key = f"{FAILURE_PREFIX}/{arxiv_id}.jsonl"
        s3_client.put_object(Bucket=BUCKET, Key=s3_key, Body=json.dumps(failure_payload))

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            message = json.loads(record['body'])
            batch_bucket = message['bucket']
            batch_key = message['key']
            
            print(f"Processing batch file: s3://{batch_bucket}/{batch_key}")
            
            batch_object = s3_client.get_object(Bucket=batch_bucket, Key=batch_key)
            batch_content = batch_object['Body'].read().decode('utf-8')
            
            papers_to_process = [json.loads(line) for line in batch_content.splitlines() if line]
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                list(executor.map(process_single_paper, papers_to_process))
            
            print(f"Finished processing batch file: {batch_key}")

        except Exception as e:
            print(f"CRITICAL: Failed to process message for batch {record.get('body')}. Error: {e}")
            raise e

    return {'statusCode': 200, 'body': 'Batch processing completed.'}