import json
import os
import boto3
import traceback
import requests
import urllib.parse

# Configuration from environment variables
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
OPENALEX_EMAIL = os.environ.get('OPENALEX_EMAIL', 'default-user@example.com')

if not S3_BUCKET_NAME:
    raise ValueError("Environment variable S3_BUCKET_NAME is not set")

# Initialize AWS S3 client
s3 = boto3.client('s3')

def handler(event, context):
    """
    Lambda handler triggered by SQS. This function parses the incoming message
    and calls the core processing logic. It's designed to differentiate between
    permanent errors (like a malformed message) and transient, retriable errors.
    """
    for record in event.get('Records', []):
        paper_id = 'unknown_id'
        original_message = record.get('body', '{}')
        
        # Safely parse the incoming message.
        try:
            message_body = json.loads(original_message)
            paper_id = message_body.get('paper_id')
            title = message_body.get('title')

            if not paper_id or not title:
                print(f"Invalid message, missing paper_id or title: {original_message}")
                log_permanent_error(paper_id, 'MalformedMessage', 'Message is missing paper_id or title', original_message)
                continue

        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON message body: {e}")
            log_permanent_error('unknown_id_parsing_error', type(e).__name__, str(e), original_message)
            continue

        print(f"Processing Paper ID: {paper_id}")
        process_paper(paper_id, title, original_message)


def process_paper(paper_id: str, title: str, original_message: str):
    """
    Processes a single paper using a two-step OpenAlex API call:
    1. Search for the work by title.
    2. Batch-fetch all author details for that work.
    """
    try:
        # Step 1: Search for the work in OpenAlex
        safe_title = urllib.parse.quote_plus(f'"{title}"')
        works_url = f"https://api.openalex.org/works?search={safe_title}&per_page=1&mailto={OPENALEX_EMAIL}"
        
        r_works = requests.get(works_url, timeout=30)
        r_works.raise_for_status()  # Raises HTTPError for 4xx/5xx responses
        works_data = r_works.json()

        if not works_data.get("results"):
            log_not_found(paper_id, title, 'No results returned from OpenAlex for this title.')
            return

        paper_data = works_data["results"][0]

        # Step 2: Extract author IDs and fetch their details in a single batch request
        author_info = []
        author_ids = [
            authorship['author']['id'].split('/')[-1] 
            for authorship in paper_data.get('authorships', []) 
            if authorship.get('author') and authorship['author'].get('id')
        ]
        
        if author_ids:
            author_id_filter = '|'.join(author_ids)
            authors_url = f"https://api.openalex.org/authors?filter=openalex_id:{author_id_filter}&per_page={len(author_ids)}&mailto={OPENALEX_EMAIL}"
            
            r_authors = requests.get(authors_url, timeout=30)
            r_authors.raise_for_status()
            authors_data = r_authors.json()
            
            # Create a lookup map for efficient access
            author_details_map = {author['id'].split('/')[-1]: author for author in authors_data.get('results', [])}
            
            # Map the h-index back to the original author order
            for authorship in paper_data.get('authorships', []):
                 if authorship.get('author') and authorship['author'].get('id'):
                    auth_id = authorship['author']['id'].split('/')[-1]
                    details = author_details_map.get(auth_id)
                    # Use the display name from the work record, but h-index from author record
                    display_name = authorship.get('author', {}).get('display_name')
                    h_index = details.get('summary_stats', {}).get('h_index') if details else None
                    author_info.append({'name': display_name, 'hIndex': h_index})

        # Step 3: Combine and save the successful result
        result = {
            'original_id': paper_id,
            'searched_title': title,
            'found_paper_title': paper_data.get('display_name'),
            'citationCount': paper_data.get('cited_by_count'),
            'authors': author_info
        }
        
        s3_key = f"citation_results/success/{paper_id}.json"
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(result, indent=2, ensure_ascii=False))
        print(f"SUCCESS: Paper ID {paper_id} processed and saved to {s3_key}")

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        # 429 (Too Many Requests) and 5xx (Server Errors) should be retried
        if status_code == 429 or status_code >= 500:
            handle_retriable_error(e, paper_id, title, original_message)
        # 404 is a definitive "Not Found"
        elif status_code == 404:
            log_not_found(paper_id, title, f'OpenAlex API returned HTTP 404.')
        # Other 4xx errors are client-side problems and should not be retried
        else:
            log_permanent_error(paper_id, f"HTTPError_{status_code}", str(e), original_message)

    except Exception as e:
        # All other exceptions (e.g., network timeouts) are treated as retriable
        handle_retriable_error(e, paper_id, title, original_message)

def log_not_found(paper_id, title, details):
    """Logs a record when a paper cannot be found."""
    not_found_info = {'paper_id': paper_id, 'title': title, 'status': 'Not Found', 'details': details}
    s3_key = f"citation_results/not_found/{paper_id}.json"
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(not_found_info, indent=2, ensure_ascii=False))
    print(f"NOT FOUND: Paper ID {paper_id}. Logged to {s3_key}")

def log_permanent_error(paper_id, error_type, error_message, original_message):
    """Logs non-retriable errors to a specific S3 path."""
    error_info = {
        'error_type': error_type,
        'error_message': error_message,
        'original_message': original_message
    }
    s3_key = f"citation_results/error/{paper_id}_permanent_error.json"
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(error_info, indent=2, ensure_ascii=False))

def handle_retriable_error(e, paper_id, title, original_message):
    """
    Logs detailed information for a retriable error and then raises the
    exception to signal failure to SQS, triggering a retry.
    """
    print(f"ERROR processing Paper ID {paper_id}: {type(e).__name__} - {str(e)}")
    error_info = {
        'paper_id': paper_id,
        'title': title,
        'error_type': type(e).__name__,
        'error_message': str(e),
        'full_traceback': traceback.format_exc(),
        'original_message': original_message
    }
    s3_key = f"citation_results/error/{paper_id}_retriable.json"
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(error_info, indent=2, ensure_ascii=False))
    print(f"Detailed retriable error for {paper_id} logged to {s3_key}")
    raise e
