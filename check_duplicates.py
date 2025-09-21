#!/usr/bin/env python3
"""
Script to check for duplicate cycle_ids in BigQuery
"""

import os
from google.cloud import bigquery
from google.oauth2 import service_account

# BigQuery configuration
PROJECT_ID = "the-wash-pie"
DATASET_ID = "seko"
TABLE_ID = "wash-cycles"

def check_duplicates():
    """Check for duplicate cycle_ids in BigQuery"""
    try:
        # Use service account key from Docker secret
        credentials_path = "/run/secrets/bigquery_credentials"
        if os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
        else:
            # Fallback to environment variable for local testing
            credentials = None
        
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        
        # Query to find duplicate cycle_ids
        query = f"""
        SELECT 
            cycle_id,
            COUNT(*) as count,
            STRING_AGG(CAST(is_completed AS STRING), ', ') as completion_statuses,
            STRING_AGG(CAST(last_updated AS STRING), ', ') as update_times
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
        GROUP BY cycle_id
        HAVING COUNT(*) > 1
        ORDER BY count DESC, cycle_id
        """
        
        print("🔍 Checking for duplicate cycle_ids in last 3 days...")
        results = client.query(query).result()
        
        duplicates = list(results)
        if duplicates:
            print(f"❌ Found {len(duplicates)} duplicate cycle_ids:")
            for row in duplicates:
                print(f"  {row.cycle_id}: {row.count} copies")
                print(f"    Completion statuses: {row.completion_statuses}")
                print(f"    Update times: {row.update_times}")
                print()
        else:
            print("✅ No duplicate cycle_ids found!")
        
        # Also show total count
        count_query = f"""
        SELECT COUNT(*) as total_rows
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
        """
        
        count_result = client.query(count_query).result()
        total_rows = list(count_result)[0].total_rows
        print(f"📊 Total rows in last 3 days: {total_rows}")
        
        return len(duplicates) == 0
        
    except Exception as e:
        print(f"❌ Failed to check duplicates: {e}")
        return False

if __name__ == "__main__":
    success = check_duplicates()
    exit(0 if success else 1)
