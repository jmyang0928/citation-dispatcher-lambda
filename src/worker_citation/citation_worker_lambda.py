import json
import os
import boto3
from semanticscholar import SemanticScholar
# --- FIX IS HERE: Import exception from the correct submodule ---
from semanticscholar.errors import PaperNotFoundException

# From environment variables
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
if not S3_BUCKET_NAME:
    raise ValueError("Environment variable S3_BUCKET_NAME is not set")

# AWS clients
s3 = boto3.client('s3')
# Initialize client in the global scope for connection reuse on warm starts
sch = SemanticScholar(timeout=30)

def handler(event, context):
    """
    Lambda handler triggered by SQS.
    """
    for record in event.get('Records', []):
        paper_id = 'unknown_id'
        try:
            message_body = json.loads(record.get('body', '{}'))
            paper_id = message_body.get('paper_id')
            title = message_body.get('title')

            if not paper_id or not title:
                print(f"Invalid message, missing paper_id or title: {record.get('body')}")
                continue

            print(f"Processing Paper ID: {paper_id}")
            
            process_paper(paper_id, title)

        except Exception as e:
            # Catch any unexpected errors during processing
            error_info = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'original_message': record.get('body')
            }
            s3_key = f"citation_results/error/{paper_id}.json"
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(error_info, indent=2, ensure_ascii=False)
            )
            print(f"An unexpected error occurred for Paper ID {paper_id}. Details logged to {s3_key}")
            # Do not re-raise the exception, so the message is removed from the queue
            # and not retried infinitely for a code bug.

def process_paper(paper_id: str, title: str):
    """
    Processes a single paper: queries Semantic Scholar and writes the result to S3.
    """
    try:
        # 1. Search for the paper
        search_results = sch.search_paper(
            query=title, 
            limit=1, 
            fields=["title", "citationCount", "authors.name", "authors.authorId"]
        )

        if not search_results or not search_results[0].get('paperId'):
            # Trigger the specific exception if no results are found
            raise PaperNotFoundException(f"No results found for title '{title}'.")

        paper = search_results[0]
        
        # 2. Get h-index for authors
        author_ids = [a['authorId'] for a in paper.get('authors', []) if a.get('authorId')]
        authors_details = []
        if author_ids:
            # Get author details in batches to be safe
            batch_size = 100
            for i in range(0, len(author_ids), batch_size):
                batch_ids = author_ids[i:i+batch_size]
                authors_details.extend(sch.get_authors(batch_ids, fields=["name", "hIndex"]))

        author_info = [{'name': a.name, 'hIndex': a.hIndex} for a in authors_details if a]

        # 3. Format and save the successful result
        result = {
            'original_id': paper_id,
            'searched_title': title,
            'found_paper_title': paper.get('title'),
            'citationCount': paper.get('citationCount'),
            'authors': author_info
        }
        
        s3_key = f"citation_results/success/{paper_id}.json"
        s3.put_object(
            Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(result, indent=2, ensure_ascii=False)
        )
        print(f"SUCCESS: Paper ID {paper_id} processed and saved to {s3_key}")

    except PaperNotFoundException as e:
        # Handle the 404 case specifically
        not_found_info = {'paper_id': paper_id, 'title': title, 'status': 'Not Found', 'details': str(e)}
        s3_key = f"citation_results/not_found/{paper_id}.json"
        s3.put_object(
            Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(not_found_info, indent=2, ensure_ascii=False)
        )
        print(f"NOT FOUND: Paper ID {paper_id}. Logged to {s3_key}")
    
    except Exception as e:
        # Handle other API errors or transient issues
        print(f"ERROR processing Paper ID {paper_id}: {type(e).__name__} - {str(e)}")
        # Re-raise the exception. This will cause the Lambda to fail, and SQS will
        # make the message visible again for a retry after the visibility timeout.
        raise e
