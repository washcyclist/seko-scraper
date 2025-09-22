import os
import time
import re
from datetime import datetime, timezone
import json
import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from google.cloud import bigquery
from google.oauth2 import service_account

# Load credentials from .env file
load_dotenv()
USERNAME = os.getenv("SEKO_USERNAME")
PASSWORD = os.getenv("SEKO_PASSWORD")
UPTIME_KUMA_URL = os.getenv("UPTIME_KUMA_PUSH_URL")

# BigQuery configuration
PROJECT_ID = "the-wash-pie"
DATASET_ID = "seko"
TABLE_ID = "wash-cycles"

def notify_uptime_kuma(status="success", message=""):
    """Send notification to Uptime Kuma"""
    if not UPTIME_KUMA_URL:
        print("⚠️ No Uptime Kuma URL configured")
        return
    
    try:
        url = f"{UPTIME_KUMA_URL}?status={status}&msg={message}"
        response = requests.get(url, timeout=10)
        print(f"📊 Uptime Kuma notification sent: {status}")
    except Exception as e:
        print(f"❌ Failed to notify Uptime Kuma: {e}")

def parse_duration_to_minutes(duration_str):
    """Convert duration string (mmm:ss) to minutes as float"""
    if not duration_str or duration_str.strip() == "":
        return None
    
    try:
        # Handle format like "002:35" or "10:45"
        parts = duration_str.strip().split(':')
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = int(parts[1])
            return round(minutes + (seconds / 60), 2)
    except (ValueError, IndexError):
        pass
    
    return None

def parse_numeric_value(value_str):
    """Extract numeric value from string, removing units"""
    if not value_str or value_str.strip() == "":
        return None
    
    try:
        # Use regex to extract first number (handles cases like "15.5 kg", "25ml", etc.)
        match = re.search(r'(\d+\.?\d*)', str(value_str).strip())
        if match:
            return float(match.group(1))
    except (ValueError, AttributeError):
        pass
    
    return None

def parse_datetime_field(datetime_str):
    """Parse the Date & Time field into start_time, end_time, duration, and completion status"""
    if not datetime_str or datetime_str.strip() == "":
        return None, None, None, False
    
    try:
        # Handle format: "2025/07/25 23:32:55 - 00:02:06" or "2025/07/26 08:04:52 - not ended"
        parts = datetime_str.strip().split(' - ')
        if len(parts) != 2:
            return None, None, None, False
        
        start_str, end_part = parts
        start_time = datetime.strptime(start_str, "%Y/%m/%d %H:%M:%S")
        start_time = start_time.replace(tzinfo=timezone.utc)
        
        if end_part.strip() == "not ended":
            return start_time, None, None, False
        else:
            # Parse duration and calculate end time
            duration_minutes = parse_duration_to_minutes(end_part)
            if duration_minutes:
                end_time = start_time.replace(second=0, microsecond=0)
                end_time = end_time.replace(minute=end_time.minute + int(duration_minutes))
                if end_time.minute >= 60:
                    end_time = end_time.replace(hour=end_time.hour + 1, minute=end_time.minute - 60)
                return start_time, end_time, duration_minutes, True
            else:
                return start_time, None, None, True
                
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

def setup_bigquery_table():
    """Create BigQuery table if it doesn't exist"""
    try:
        # Use service account key from Docker secret
        credentials_path = "/run/secrets/bigquery_credentials"
        if os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
        else:
            # Fallback to environment variable for local testing
            credentials = None
        
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        
        # Define table schema
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
        SELECT cycle_id, is_completed
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
        """
        
        results = client.query(query).result()
        existing_cycles = {}
        for row in results:
            existing_cycles[row.cycle_id] = row.is_completed
        
        print(f"📊 Found {len(existing_cycles)} existing cycles in last 2 days")
        return existing_cycles
    
    except Exception as e:
        print(f"⚠️ Failed to get existing cycles: {e}")
        return {}

def transform_row_data(row_data):
    """Transform scraped row data into BigQuery format"""
    if len(row_data) < 14:
        print(f"⚠️ Row has insufficient columns: {len(row_data)}")
        return None
    
    # Parse the datetime field
    start_time, end_time, duration_minutes, is_completed = parse_datetime_field(row_data[0])
    if not start_time:
        print(f"⚠️ Could not parse datetime from: {row_data[0]}")
        return None
    
    # Create cycle ID
    cycle_id = create_cycle_id(start_time, row_data[1])
    if not cycle_id:
        print(f"⚠️ Could not create cycle ID")
        return None
    
    # Transform data - convert datetime objects to ISO format strings
    transformed = {
        "cycle_id": cycle_id,
        "start_time": start_time.isoformat(),  # Convert to ISO string
        "end_time": end_time.isoformat() if end_time else None,  # Convert to ISO string or None
        "duration_minutes": duration_minutes,
        "device_name": row_data[1] if row_data[1] else None,
        "formula_name": row_data[2] if row_data[2] else None,
        "washer": row_data[3] if row_data[3] else None,
        "customer": row_data[4] if row_data[4] else None,
        "weight_numeric": parse_numeric_value(row_data[6]),
        "optin_flex": parse_numeric_value(row_data[7]),
        "optin_alka": parse_numeric_value(row_data[8]),
        "optin_proxy": parse_numeric_value(row_data[9]),
        "optin_citra": parse_numeric_value(row_data[10]),
        "viva_turbulent": parse_numeric_value(row_data[11]),
        "excess_time_minutes": parse_duration_to_minutes(row_data[12]),
        "idle_time_minutes": parse_duration_to_minutes(row_data[13]),
        "is_completed": is_completed,
        "last_updated": datetime.now(timezone.utc).isoformat(),  # Convert to ISO string
    }
    
    return transformed

def upload_to_bigquery(client, new_rows, updated_rows):
    """Upload transformed rows to BigQuery with proper deduplication"""
    if not new_rows and not updated_rows:
        print("📊 No new rows to upload")
        return

    try:
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        table = client.get_table(table_ref)

        # Handle new rows - use load job instead of streaming inserts
        if new_rows:
            print(f"📤 Using load job to insert {len(new_rows)} new rows (avoids streaming buffer)...")

            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

            # Define schema to ensure proper data types
            job_config.schema = [
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

            load_job = client.load_table_from_json(new_rows, table, job_config=job_config)
            load_job.result()  # Wait for completion
            print(f"✅ Successfully inserted {len(new_rows)} new rows using load job")

        # Handle updated rows using MERGE to avoid streaming buffer issues
        if updated_rows:
            print(f"🔄 Using MERGE to update {len(updated_rows)} rows (avoids streaming buffer issues)...")

            # Create a temporary table with the updated data
            temp_table_id = f"{TABLE_ID}_temp_{int(datetime.now().timestamp())}"
            temp_table_ref = client.dataset(DATASET_ID).table(temp_table_id)

            try:
                # Create temporary table with updated rows
                job_config = bigquery.LoadJobConfig()
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

                # Define schema to ensure proper data types
                job_config.schema = [
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

                load_job = client.load_table_from_json(
                    updated_rows, temp_table_ref, job_config=job_config
                )
                load_job.result()  # Wait for completion

                # Use MERGE to update existing rows
                merge_query = f"""
                MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS target
                USING `{PROJECT_ID}.{DATASET_ID}.{temp_table_id}` AS source
                ON target.cycle_id = source.cycle_id
                WHEN MATCHED THEN
                  UPDATE SET
                    start_time = source.start_time,
                    end_time = source.end_time,
                    duration_minutes = source.duration_minutes,
                    device_name = source.device_name,
                    formula_name = source.formula_name,
                    washer = source.washer,
                    customer = source.customer,
                    weight_numeric = source.weight_numeric,
                    optin_flex = source.optin_flex,
                    optin_alka = source.optin_alka,
                    optin_proxy = source.optin_proxy,
                    optin_citra = source.optin_citra,
                    viva_turbulent = source.viva_turbulent,
                    excess_time_minutes = source.excess_time_minutes,
                    idle_time_minutes = source.idle_time_minutes,
                    is_completed = source.is_completed,
                    last_updated = source.last_updated
                WHEN NOT MATCHED THEN
                  INSERT (cycle_id, start_time, end_time, duration_minutes, device_name, formula_name, washer, customer, weight_numeric, optin_flex, optin_alka, optin_proxy, optin_citra, viva_turbulent, excess_time_minutes, idle_time_minutes, is_completed, last_updated)
                  VALUES (source.cycle_id, source.start_time, source.end_time, source.duration_minutes, source.device_name, source.formula_name, source.washer, source.customer, source.weight_numeric, source.optin_flex, source.optin_alka, source.optin_proxy, source.optin_citra, source.viva_turbulent, source.excess_time_minutes, source.idle_time_minutes, source.is_completed, source.last_updated)
                """

                merge_job = client.query(merge_query)
                merge_job.result()  # Wait for completion

                print(f"✅ Successfully updated {len(updated_rows)} rows using MERGE")

            finally:
                # Clean up temporary table
                try:
                    client.delete_table(temp_table_ref)
                except Exception:
                    pass  # Ignore cleanup errors

        total_uploaded = len(new_rows) + len(updated_rows)
        print(f"✅ Successfully processed {total_uploaded} total rows to BigQuery")

    except Exception as e:
        print(f"❌ Failed to upload to BigQuery: {e}")
        raise

def main():
    start_time = datetime.now()
    script_version = "v2.3-no-streaming-2025-09-22"
    print(f"🚀 Starting SEKO cycles scrape at {start_time}")
    print(f"📋 Script version: {script_version}")
    print(f"🔧 Deduplication: ENABLED (2-day window, load jobs + MERGE, no streaming buffer)")
    
    try:
        # Setup BigQuery
        client = setup_bigquery_table()
        existing_cycles = get_existing_cycle_ids(client)
        
        # Scrape data
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            # Login
            page.goto("https://www.sekoweb.com/login")
            page.fill("#username", USERNAME)
            page.fill("#password", PASSWORD)
            page.press("#password", "Enter")
            page.wait_for_load_state("networkidle")
            time.sleep(2)

            # Navigate to cycles
            page.click('div#role0f72b56c-d784-42ea-a8c7-069f572a3874')
            page.wait_for_timeout(2000)
            page.click('a#statistics_tab')
            page.wait_for_timeout(1000)
            page.click('a.stat-tab[href="#laundry-cycles"]')
            page.wait_for_timeout(2500)

            # For hourly updates, get today's data plus yesterday to catch delayed entries
            page.click('label:has-text("Today")')
            page.wait_for_timeout(2000)
            page.select_option('select[name="cycletable_length"]', value='-1')
            page.wait_for_timeout(2300)
            
            # Get today's data first
            page.wait_for_selector('#cycletable', state='visible')
            page.wait_for_timeout(2500)
            
            today_rows = page.eval_on_selector_all(
                '#cycletable tbody tr',
                '''
                    rows => rows.map(row => {
                    const cells = Array.from(row.querySelectorAll("td"));
                    return cells.map(cell => cell.innerText.trim());
                    })
                '''
            )
            
            print(f"📊 Found {len(today_rows)} rows for today")
            
            # Also get yesterday's data to catch any delayed completions
            print("📅 Getting yesterday's data...")
            page.click('label:has-text("Yesterday")')
            page.wait_for_timeout(2000)
            page.select_option('select[name="cycletable_length"]', value='-1')
            page.wait_for_timeout(2300)

            # Wait for table and extract yesterday's data
            page.wait_for_selector('#cycletable', state='visible')
            page.wait_for_timeout(2500)

            # Extract yesterday's rows
            yesterday_rows = page.eval_on_selector_all(
                '#cycletable tbody tr',
                '''
                    rows => rows.map(row => {
                    const cells = Array.from(row.querySelectorAll("td"));
                    return cells.map(cell => cell.innerText.trim());
                    })
                '''
            )
            
            print(f"📊 Found {len(yesterday_rows)} rows for yesterday")
            
            # Combine both datasets
            rows = today_rows + yesterday_rows
            print(f"📊 Total combined rows: {len(rows)}")

            browser.close()

        # Process and filter rows
        new_rows = []
        updated_rows = []
        
        for row_data in rows:
            transformed = transform_row_data(row_data)
            if not transformed:
                continue
            
            cycle_id = transformed["cycle_id"]
            
            # Check if this is new or needs updating
            if cycle_id not in existing_cycles:
                # Completely new cycle
                print(f"🆕 New cycle: {cycle_id} (completed: {transformed['is_completed']})")
                new_rows.append(transformed)
            elif existing_cycles[cycle_id] != transformed["is_completed"]:
                # Completion status changed - update it
                print(f"🔄 Update cycle: {cycle_id} ({existing_cycles[cycle_id]} → {transformed['is_completed']})")
                updated_rows.append(transformed)
            else:
                # Skip if cycle exists and completion status hasn't changed
                print(f"⏭️ Skip cycle: {cycle_id} (no change, completed: {transformed['is_completed']})")
        
        # Upload new and updated rows with proper deduplication
        if new_rows or updated_rows:
            upload_to_bigquery(client, new_rows, updated_rows)
            
        duration = datetime.now() - start_time
        total_uploaded = len(new_rows) + len(updated_rows)
        message = f"v2.3-no-streaming: Processed {len(rows)} rows, uploaded {total_uploaded} ({len(new_rows)} new, {len(updated_rows)} updated)"
        print(f"✅ {message} in {duration.total_seconds():.1f}s")
        notify_uptime_kuma("up", message)
        
    except Exception as e:
        error_msg = f"SEKO scrape failed: {str(e)}"
        print(f"❌ {error_msg}")
        notify_uptime_kuma("down", error_msg)
        raise

if __name__ == "__main__":
    main()