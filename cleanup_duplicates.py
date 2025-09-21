#!/usr/bin/env python3
"""
Script to clean up duplicate cycle_ids in BigQuery by keeping only the most recent version
"""

import os
from google.cloud import bigquery
from google.oauth2 import service_account

# BigQuery configuration
PROJECT_ID = "the-wash-pie"
DATASET_ID = "seko"
TABLE_ID = "wash-cycles"

def cleanup_duplicates():
    """Remove duplicate cycle_ids, keeping only the most recent version"""
    try:
        # Use service account key from Docker secret
        credentials_path = "/run/secrets/bigquery_credentials"
        if os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
        else:
            # Fallback to environment variable for local testing
            credentials = None
        
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        
        print("🧹 Starting duplicate cleanup process...")
        
        # First, let's see how many duplicates we have
        count_query = f"""
        SELECT COUNT(*) as duplicate_count
        FROM (
            SELECT cycle_id, COUNT(*) as count
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            GROUP BY cycle_id
            HAVING COUNT(*) > 1
        )
        """
        
        count_result = client.query(count_query).result()
        duplicate_count = list(count_result)[0].duplicate_count
        print(f"📊 Found {duplicate_count} cycle_ids with duplicates")
        
        if duplicate_count == 0:
            print("✅ No duplicates found!")
            return True
        
        # Since we can't DELETE from streaming buffer, create a clean table and replace
        print("🔄 Creating deduplicated table (streaming buffer prevents direct DELETE)...")

        # Create a new table with deduplicated data
        create_clean_query = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}_clean` AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY cycle_id
                    ORDER BY last_updated DESC,
                             CASE WHEN is_completed THEN 0 ELSE 1 END ASC
                ) as row_num
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        )
        WHERE row_num = 1
        """

        create_job = client.query(create_clean_query)
        create_job.result()

        # Drop original table and rename clean table
        print("� Replacing original table with clean version...")
        drop_query = f"DROP TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        drop_job = client.query(drop_query)
        drop_job.result()

        rename_query = f"""
        ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}_clean`
        RENAME TO `{TABLE_ID}`
        """
        rename_job = client.query(rename_query)
        rename_job.result()

        print(f"✅ Successfully replaced table with deduplicated version")
        
        print("✅ Duplicate cleanup completed successfully!")
        
        # Verify no duplicates remain
        verify_query = f"""
        SELECT COUNT(*) as remaining_duplicates
        FROM (
            SELECT cycle_id, COUNT(*) as count
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            GROUP BY cycle_id
            HAVING COUNT(*) > 1
        )
        """
        
        verify_result = client.query(verify_query).result()
        remaining = list(verify_result)[0].remaining_duplicates
        
        if remaining == 0:
            print("✅ Verification passed: No duplicates remain!")
            return True
        else:
            print(f"❌ Verification failed: {remaining} duplicates still exist!")
            return False
        
    except Exception as e:
        print(f"❌ Failed to cleanup duplicates: {e}")
        return False

if __name__ == "__main__":
    success = cleanup_duplicates()
    exit(0 if success else 1)
