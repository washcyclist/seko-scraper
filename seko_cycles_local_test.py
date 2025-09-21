import os
import time
import re
import argparse
from datetime import datetime, timezone, timedelta
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

# Configuration flags (will be set by command line arguments)
LOCAL_TEST = True
TARGET_DATES = []

def notify_uptime_kuma(status="success", message=""):
    """Send notification to Uptime Kuma"""
    if not UPTIME_KUMA_URL:
        print("⚠️ No Uptime Kuma URL configured")
        return
    
    if LOCAL_TEST:
        print(f"🧪 [LOCAL TEST] Would notify Uptime Kuma: {status} - {message}")
        return
    
    try:
        url = f"{UPTIME_KUMA_URL}?status={status}&msg={message}"
        requests.get(url, timeout=10)
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
        # Use local credentials file for testing
        credentials_path = "./credentials.json"
        if os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            print("✅ Using local credentials.json file")
        else:
            print("❌ Local credentials.json file not found")
            return None
        
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
            if LOCAL_TEST:
                print(f"🧪 [LOCAL TEST] Would create BigQuery table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
            else:
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
        WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        """
        
        if LOCAL_TEST:
            print(f"🧪 [LOCAL TEST] Would query: {query}")
            return {}
        
        results = client.query(query).result()
        existing_cycles = {}
        for row in results:
            existing_cycles[row.cycle_id] = row.is_completed
        
        print(f"📊 Found {len(existing_cycles)} existing cycles in last 7 days")
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
    
    # Transform data - convert datetime objects to ISO format strings for BigQuery
    transformed = {
        "cycle_id": cycle_id,
        "start_time": start_time.isoformat() if start_time else None,
        "end_time": end_time.isoformat() if end_time else None,
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
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    
    return transformed

def upload_to_bigquery(client, rows_to_upload):
    """Upload transformed rows to BigQuery"""
    if not rows_to_upload:
        print("📊 No new rows to upload")
        return
    
    if LOCAL_TEST:
        print(f"🧪 [LOCAL TEST] Would upload {len(rows_to_upload)} rows to BigQuery:")
        for row in rows_to_upload[:3]:  # Show first 3 rows
            print(f"   - {row['cycle_id']}: {row['device_name']} at {row['start_time']}")
        if len(rows_to_upload) > 3:
            print(f"   ... and {len(rows_to_upload) - 3} more rows")
        return
    
    try:
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        table = client.get_table(table_ref)
        
        # Insert rows
        errors = client.insert_rows_json(table, rows_to_upload)
        
        if not errors:
            print(f"✅ Successfully uploaded {len(rows_to_upload)} rows to BigQuery")
        else:
            print(f"❌ BigQuery upload errors: {errors}")
            raise Exception(f"BigQuery upload failed: {errors}")
    
    except Exception as e:
        print(f"❌ Failed to upload to BigQuery: {e}")
        raise

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='SEKO Cycles Data Scraper with BigQuery Upload')
    parser.add_argument('--start-date',
                       help='Start date to scrape (YYYY-MM-DD format). Example: --start-date 2024-01-15')
    parser.add_argument('--end-date',
                       help='End date to scrape (YYYY-MM-DD format). Example: --end-date 2024-01-16')
    parser.add_argument('--upload', action='store_true',
                       help='Actually upload to BigQuery (default is test mode)')
    parser.add_argument('--headless', action='store_true', default=False,
                       help='Run browser in headless mode')

    args = parser.parse_args()

    # Parse and validate dates
    start_date = None
    end_date = None

    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        except ValueError:
            print(f"❌ Invalid start date format: {args.start_date}. Use YYYY-MM-DD format.")
            exit(1)

    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        except ValueError:
            print(f"❌ Invalid end date format: {args.end_date}. Use YYYY-MM-DD format.")
            exit(1)

    # Validate date range
    if start_date and end_date and start_date > end_date:
        print(f"❌ Start date ({start_date}) cannot be after end date ({end_date})")
        exit(1)

    # Set defaults if not provided
    if not start_date and not end_date:
        # Default to yesterday and today
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        start_date = yesterday
        end_date = today
    elif start_date and not end_date:
        # If only start date provided, use same date as end date
        end_date = start_date
    elif end_date and not start_date:
        # If only end date provided, use same date as start date
        start_date = end_date

    return start_date, end_date, not args.upload, not args.headless

def select_date_range_on_page(page, start_date, end_date):
    """Select a date range on the SEKO web interface"""
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    print(f"🔍 DEBUG: Today is {today}, Yesterday is {yesterday}")
    print(f"🔍 DEBUG: Requested range is {start_date} to {end_date}")

    # Check if we can use the predefined options
    if start_date == today and end_date == today:
        print(f"📅 Selecting 'Today' ({start_date})")
        page.click('label:has-text("Today")')
    elif start_date == yesterday and end_date == yesterday:
        print(f"📅 Selecting 'Yesterday' ({start_date})")
        page.click('label:has-text("Yesterday")')
    elif start_date == yesterday and end_date == today:
        print(f"📅 Selecting 'Yesterday' and 'Today' range ({start_date} to {end_date})")
        # For yesterday to today, we'll use custom range
        page.click('label:has-text("Custom")')
        page.wait_for_timeout(1000)

        # Format the date range as YYYY/MM/DD - YYYY/MM/DD
        start_str = start_date.strftime('%Y/%m/%d')
        end_str = end_date.strftime('%Y/%m/%d')
        date_range_str = f"{start_str} - {end_str}"

        print(f"📅 Setting date range to: {date_range_str}")

        # Clear and fill the date input field
        page.fill('input[name="chemical_date_range"]', date_range_str)
        page.wait_for_timeout(1000)

        # Verify what was actually set
        actual_value = page.input_value('input[name="chemical_date_range"]')
        print(f"🔍 DEBUG: Date field value after setting: '{actual_value}'")

        # Click the OK button in the date picker
        try:
            page.click('button.applyBtn.btn.btn-sm.btn-success')
            print("📅 Clicked OK button in date picker")
            page.wait_for_timeout(2000)
        except Exception as e:
            print(f"⚠️ Could not click OK button: {e}")
            # Try alternative selector
            try:
                page.click('.applyBtn')
                print("📅 Clicked OK button (alternative selector)")
                page.wait_for_timeout(2000)
            except Exception as e2:
                print(f"❌ Failed to click OK button: {e2}")

        # Click the Apply button to refresh the data
        try:
            page.click('a[href="javascript:refreshStatisticVisibleComponent()"]')
            print("📅 Clicked Apply button to refresh data")
            page.wait_for_timeout(5000)  # Wait longer for data to load
        except Exception as e:
            print(f"⚠️ Could not click Apply button: {e}")
            # Try alternative selector
            try:
                page.click('a.btn.btn-sm.btn-success:has-text("Apply")')
                print("📅 Clicked Apply button (alternative selector)")
                page.wait_for_timeout(5000)
            except Exception as e2:
                print(f"❌ Failed to click Apply button: {e2}")
    else:
        # For any other custom date range
        print(f"📅 Selecting custom date range: {start_date} to {end_date}")

        # Click on "Custom" date range option
        page.click('label:has-text("Custom")')
        page.wait_for_timeout(1000)

        # Format the date range as YYYY/MM/DD - YYYY/MM/DD
        start_str = start_date.strftime('%Y/%m/%d')
        end_str = end_date.strftime('%Y/%m/%d')
        date_range_str = f"{start_str} - {end_str}"

        print(f"📅 Setting date range to: {date_range_str}")

        # Clear and fill the date input field
        page.fill('input[name="chemical_date_range"]', date_range_str)
        page.wait_for_timeout(1000)

        # Verify what was actually set
        actual_value = page.input_value('input[name="chemical_date_range"]')
        print(f"🔍 DEBUG: Date field value after setting: '{actual_value}'")

        # Click the OK button in the date picker
        try:
            page.click('button.applyBtn.btn.btn-sm.btn-success')
            print("📅 Clicked OK button in date picker")
            page.wait_for_timeout(2000)
        except Exception as e:
            print(f"⚠️ Could not click OK button: {e}")
            # Try alternative selector
            try:
                page.click('.applyBtn')
                print("📅 Clicked OK button (alternative selector)")
                page.wait_for_timeout(2000)
            except Exception as e2:
                print(f"❌ Failed to click OK button: {e2}")

        # Click the Apply button to refresh the data
        try:
            page.click('a[href="javascript:refreshStatisticVisibleComponent()"]')
            print("📅 Clicked Apply button to refresh data")
            page.wait_for_timeout(5000)  # Wait longer for data to load
        except Exception as e:
            print(f"⚠️ Could not click Apply button: {e}")
            # Try alternative selector
            try:
                page.click('a.btn.btn-sm.btn-success:has-text("Apply")')
                print("📅 Clicked Apply button (alternative selector)")
                page.wait_for_timeout(5000)
            except Exception as e2:
                print(f"❌ Failed to click Apply button: {e2}")

    page.wait_for_timeout(2000)



def main():
    global LOCAL_TEST

    # Parse command line arguments
    start_date, end_date, local_test_mode, headless_mode = parse_arguments()
    LOCAL_TEST = local_test_mode

    start_time = datetime.now()
    print(f"🚀 Starting SEKO cycles scrape at {start_time}")
    print(f"📅 Date range: {start_date} to {end_date}")

    if LOCAL_TEST:
        print("🧪 Running in LOCAL TEST mode (no actual uploads)")
    else:
        print("🚀 Running in PRODUCTION mode (will upload to BigQuery)")

    try:
        # Setup BigQuery
        client = setup_bigquery_table()
        existing_cycles = get_existing_cycle_ids(client)
        
        # Scrape data
        with sync_playwright() as p:
            # Launch browser - headless_mode is True when we want headless, False when we want visible
            browser = p.chromium.launch(headless=headless_mode, slow_mo=1000 if not headless_mode else 0)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            if not headless_mode:
                print("🖥️ Browser launched in visible mode - you should see the browser window")

            print("🔐 Logging into SEKO...")
            # Login
            page.goto("https://www.sekoweb.com/login")
            page.fill("#username", USERNAME)
            page.fill("#password", PASSWORD)
            page.press("#password", "Enter")
            page.wait_for_load_state("networkidle")
            time.sleep(2)

            print("🔧 Navigating to cycles...")
            # Navigate to cycles
            page.click('div#role0f72b56c-d784-42ea-a8c7-069f572a3874')
            page.wait_for_timeout(2000)
            page.click('a#statistics_tab')
            page.wait_for_timeout(1000)
            page.click('a.stat-tab[href="#laundry-cycles"]')
            page.wait_for_timeout(2500)

            # Select the date range and collect data
            print(f"📅 Processing date range: {start_date} to {end_date}")

            # Select the date range
            select_date_range_on_page(page, start_date, end_date)

            # Set table to show all rows
            page.select_option('select[name="cycletable_length"]', value='-1')
            page.wait_for_timeout(2300)

            # Wait for table and extract data
            page.wait_for_selector('#cycletable', state='visible')
            page.wait_for_timeout(2500)

            rows = page.eval_on_selector_all(
                '#cycletable tbody tr',
                '''
                    rows => rows.map(row => {
                    const cells = Array.from(row.querySelectorAll("td"));
                    return cells.map(cell => cell.innerText.trim());
                    })
                '''
            )

            print(f"📊 Found {len(rows)} rows for date range {start_date} to {end_date}")
            browser.close()

        print(f"🔄 Processing {len(rows)} scraped rows...")
        # Process and filter rows
        new_rows = []
        updated_rows = []

        for i, row_data in enumerate(rows):
            transformed = transform_row_data(row_data)
            if not transformed:
                continue

            cycle_id = transformed["cycle_id"]

            # Check if this is new or an update to incomplete cycle
            if cycle_id not in existing_cycles:
                new_rows.append(transformed)
                if i < 5:  # Show details for first few rows
                    print(f"   ➕ New: {cycle_id}")
            elif not existing_cycles[cycle_id] and transformed["is_completed"]:
                # This was incomplete before, now it's complete - update it
                updated_rows.append(transformed)
                print(f"   🔄 Updated: {cycle_id} (now complete)")

        # Upload new and updated rows
        all_uploads = new_rows + updated_rows
        if all_uploads:
            upload_to_bigquery(client, all_uploads)
        else:
            print("📊 No new or updated rows to upload")

        duration = datetime.now() - start_time
        message = f"Processed {len(rows)} rows, uploaded {len(all_uploads)} ({len(new_rows)} new, {len(updated_rows)} updated)"
        print(f"✅ {message} in {duration.total_seconds():.1f}s")

        if not LOCAL_TEST:
            notify_uptime_kuma("up", message)

    except Exception as e:
        error_msg = f"SEKO scrape failed: {str(e)}"
        print(f"❌ {error_msg}")
        if not LOCAL_TEST:
            notify_uptime_kuma("down", error_msg)
        raise

if __name__ == "__main__":
    main()