#!/usr/bin/env python3
"""
Script to upload Seko Web-9.csv data to BigQuery
"""

import os
import csv
import re
from datetime import datetime, timezone
from google.cloud import bigquery
from google.oauth2 import service_account

# BigQuery configuration
PROJECT_ID = "the-wash-pie"
DATASET_ID = "seko"
TABLE_ID = "wash-cycles"

def parse_duration_to_minutes(duration_str):
    """Convert duration string (mmm:ss) to minutes as float"""
    if not duration_str or duration_str.strip() == "" or duration_str.strip() == "-":
        return None
    
    try:
        # Handle format like "26:00" or "002:35"
        clean_str = duration_str.strip()
        parts = clean_str.split(':')
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = int(parts[1])
            return round(minutes + (seconds / 60), 2)
    except (ValueError, IndexError):
        pass
    
    return None

def parse_numeric_value(value_str):
    """Extract numeric value from string, removing units"""
    if not value_str or value_str.strip() == "" or value_str.strip() == "-":
        return None
    
    try:
        # Use regex to extract first number (handles cases like "130 lbs", "4 Oz", etc.)
        match = re.search(r'(\d+\.?\d*)', str(value_str).strip())
        if match:
            return float(match.group(1))
    except (ValueError, AttributeError):
        pass
    
    return None

def parse_csv_datetime_field(datetime_str):
    """Parse CSV Date & Time field into start_time, end_time, duration, and completion status"""
    if not datetime_str or datetime_str.strip() == "":
        return None, None, None, False

    try:
        # Handle format: "2025/05/01 04:26:44 - 04:55:54"
        parts = datetime_str.strip().split(' - ')
        if len(parts) != 2:
            return None, None, None, False

        start_str, end_str = parts

        # First try to parse the start time
        try:
            start_time = datetime.strptime(start_str, "%Y/%m/%d %H:%M:%S")
        except ValueError as e:
            # Handle invalid dates by skipping them
            if "day is out of range for month" in str(e):
                return None, None, None, False
            raise

        start_time = start_time.replace(tzinfo=timezone.utc)

        # Parse end time - it's just the time part, same date as start
        end_time_str = f"{start_str.split(' ')[0]} {end_str}"
        try:
            end_time = datetime.strptime(end_time_str, "%Y/%m/%d %H:%M:%S")
        except ValueError as e:
            # Handle invalid dates by skipping them
            if "day is out of range for month" in str(e):
                return None, None, None, False
            raise

        end_time = end_time.replace(tzinfo=timezone.utc)

        # Calculate duration in minutes
        duration_seconds = (end_time - start_time).total_seconds()
        duration_minutes = round(duration_seconds / 60, 2)

        # If end time is before start time, it means it crossed midnight
        if end_time < start_time:
            from datetime import timedelta
            end_time = end_time + timedelta(days=1)
            duration_seconds = (end_time - start_time).total_seconds()
            duration_minutes = round(duration_seconds / 60, 2)

        return start_time, end_time, duration_minutes, True

    except (ValueError, TypeError) as e:
        print(f"⚠️ Failed to parse datetime '{datetime_str}': {e}")
        return None, None, None, False

def create_cycle_id(start_time, device_name):
    """Create unique cycle ID from start time and device name"""
    if not start_time or not device_name:
        return None
    
    timestamp_str = start_time.strftime("%Y%m%d_%H%M%S")
    clean_device = re.sub(r'[^a-zA-Z0-9]', '_', str(device_name).strip())
    return f"{timestamp_str}_{clean_device}"

def parse_excess_time(excess_str):
    """Parse excess time format like '00:01 [25:59]' to get just the excess part"""
    if not excess_str or excess_str.strip() == "" or excess_str.strip() == "-":
        return None
    
    try:
        # Extract the first part before the bracket
        parts = excess_str.strip().split(' [')
        if len(parts) >= 1:
            return parse_duration_to_minutes(parts[0])
    except:
        pass
    
    return None

def parse_idle_time(idle_str):
    """Parse idle time, handling '<1' format"""
    if not idle_str or idle_str.strip() == "" or idle_str.strip() == "-":
        return None
    
    if idle_str.strip() == "<1":
        return 0.5  # Assume less than 1 minute means 0.5 minutes
    
    return parse_duration_to_minutes(idle_str)

def setup_bigquery_table():
    """Create BigQuery table if it doesn't exist"""
    try:
        # Use service account key from credentials.json file
        credentials_path = "credentials.json"
        if os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
        else:
            # Fallback to default credentials
            credentials = None
        
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        
        # Define table schema (same as existing scripts)
        schema = [
            bigquery.SchemaField("cycle_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("start_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("end_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("duration_minutes", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("device_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("formula_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("washer", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("customer", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("weight_numeric", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("optin_flex", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("optin_alka", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("optin_proxy", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("optin_citra", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("viva_turbulent", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("excess_time_minutes", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("idle_time_minutes", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("is_completed", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        
        try:
            table = client.get_table(table_ref)
            print(f"✅ BigQuery table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID} already exists")
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            print(f"✅ Created BigQuery table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        
        return client
    
    except Exception as e:
        print(f"❌ Failed to setup BigQuery: {e}")
        raise

def get_existing_cycle_ids(client):
    """Get existing cycle IDs from BigQuery to avoid duplicates"""
    try:
        query = f"""
        SELECT cycle_id
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        """
        
        results = client.query(query).result()
        existing_cycles = set()
        for row in results:
            existing_cycles.add(row.cycle_id)
        
        print(f"📊 Found {len(existing_cycles)} existing cycles in BigQuery")
        return existing_cycles
    
    except Exception as e:
        print(f"⚠️ Failed to get existing cycles: {e}")
        return set()

def transform_csv_row(row):
    """Transform CSV row data into BigQuery format"""
    if len(row) < 20:  # CSV has 20 columns
        print(f"⚠️ Row has insufficient columns: {len(row)} (expected 20)")
        return None

    # Skip the totals row
    if "Cycles Totals:" in row[0]:
        return None

    # Parse the datetime field
    start_time, end_time, duration_minutes, is_completed = parse_csv_datetime_field(row[0])
    if not start_time:
        print(f"⚠️ Could not parse datetime from: {row[0]}")
        return None

    # Create cycle ID
    cycle_id = create_cycle_id(start_time, row[1])
    if not cycle_id:
        print(f"⚠️ Could not create cycle ID")
        return None

    # Transform data - CSV column mapping:
    # 0: Date & Time, 1: Device name, 2: Formula name, 3: Washer, 4: Customer
    # 5: Duration, 6: Weight, 7: Zeplex, 8: Alkaline booster, 9: Peroxide, 10: Neutrix-sour
    # 11: OPTIN FLEX, 12: OPTIN ALKA, 13: OPTIN PROXY, 14: OPTIN CITRA, 15: OPTIN DEGREE
    # 16: VIVA GENIUX, 17: VIVA TURBULENT, 18: Excess Time, 19: IDLE Time
    transformed = {
        "cycle_id": cycle_id,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat() if end_time else None,
        "duration_minutes": duration_minutes,
        "device_name": row[1] if row[1] else None,
        "formula_name": row[2] if row[2] else None,
        "washer": row[3] if row[3] else None,
        "customer": row[4] if row[4] else None,
        "weight_numeric": parse_numeric_value(row[6]),   # "Weight" column
        "optin_flex": parse_numeric_value(row[11]),      # "OPTIN FLEX" column
        "optin_alka": parse_numeric_value(row[12]),      # "OPTIN ALKA" column
        "optin_proxy": parse_numeric_value(row[13]),     # "OPTIN PROXY" column
        "optin_citra": parse_numeric_value(row[14]),     # "OPTIN CITRA" column
        "viva_turbulent": parse_numeric_value(row[17]),  # "VIVA TURBULENT" column
        "excess_time_minutes": parse_excess_time(row[18]), # "Excess Time (mmm:ss) [Avg]" column
        "idle_time_minutes": parse_idle_time(row[19]) if len(row) > 19 else None,  # "IDLE Time (mmm:ss)" column
        "is_completed": is_completed,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }

    return transformed

def upload_to_bigquery_batch(client, rows_batch):
    """Upload a batch of rows to BigQuery"""
    try:
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        table = client.get_table(table_ref)
        
        errors = client.insert_rows_json(table, rows_batch)
        if errors:
            print(f"❌ BigQuery insert errors: {errors}")
            return False
        
        return True
    
    except Exception as e:
        print(f"❌ Failed to upload batch to BigQuery: {e}")
        return False

def main(dry_run=False):
    start_time = datetime.now()
    print(f"🚀 Starting CSV upload to BigQuery at {start_time}")

    if dry_run:
        print("🧪 DRY RUN MODE - No data will be uploaded to BigQuery")

    try:
        if not dry_run:
            # Setup BigQuery
            client = setup_bigquery_table()
            existing_cycles = get_existing_cycle_ids(client)
        else:
            client = None
            existing_cycles = set()
        
        # Read and process CSV
        csv_file = "Seko Web-9.csv"
        if not os.path.exists(csv_file):
            print(f"❌ CSV file not found: {csv_file}")
            return
        
        new_rows = []
        skipped_count = 0
        error_count = 0
        
        print(f"📖 Reading CSV file: {csv_file}")
        
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)  # Skip header
            print(f"📋 CSV Header: {header[:5]}...")  # Show first 5 columns
            
            for row_num, row in enumerate(csv_reader, start=2):
                if row_num % 5000 == 0:
                    print(f"📊 Processing row {row_num}...")
                
                transformed = transform_csv_row(row)
                if not transformed:
                    error_count += 1
                    continue
                
                cycle_id = transformed["cycle_id"]
                
                # Check if this cycle already exists
                if cycle_id in existing_cycles:
                    skipped_count += 1
                    continue
                
                new_rows.append(transformed)
                
                # Upload in batches of 1000 to avoid memory issues
                if len(new_rows) >= 1000:
                    if dry_run:
                        print(f"🧪 [DRY RUN] Would upload batch of {len(new_rows)} rows")
                        # Show sample of first few rows
                        for i, row in enumerate(new_rows[:3]):
                            print(f"   Sample {i+1}: {row['cycle_id']} - {row['device_name']} at {row['start_time']}")
                    else:
                        print(f"📤 Uploading batch of {len(new_rows)} rows...")
                        if upload_to_bigquery_batch(client, new_rows):
                            print(f"✅ Successfully uploaded batch")
                        else:
                            print(f"❌ Failed to upload batch")
                            break
                    new_rows = []

        # Upload remaining rows
        if new_rows:
            if dry_run:
                print(f"🧪 [DRY RUN] Would upload final batch of {len(new_rows)} rows")
                # Show sample of first few rows
                for i, row in enumerate(new_rows[:3]):
                    print(f"   Sample {i+1}: {row['cycle_id']} - {row['device_name']} at {row['start_time']}")
            else:
                print(f"📤 Uploading final batch of {len(new_rows)} rows...")
                if upload_to_bigquery_batch(client, new_rows):
                    print(f"✅ Successfully uploaded final batch")
                else:
                    print(f"❌ Failed to upload final batch")
        
        duration = datetime.now() - start_time
        print(f"✅ CSV upload completed in {duration.total_seconds():.1f}s")
        print(f"📊 Skipped {skipped_count} existing cycles")
        print(f"⚠️ {error_count} rows had parsing errors")
        
    except Exception as e:
        print(f"❌ CSV upload failed: {e}")
        raise

if __name__ == "__main__":
    import sys
    dry_run = "--dry-run" in sys.argv
    main(dry_run=dry_run)
