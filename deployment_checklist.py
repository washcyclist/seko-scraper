#!/usr/bin/env python3
"""
Deployment checklist to verify the new script version
"""

def print_checklist():
    print("🚀 SEKO Script Deployment Checklist")
    print("=" * 50)
    
    print("\n1. 📋 UPDATE PRODUCTION SCRIPT:")
    print("   □ Copy the updated seko_cycles_bigquery.py to production")
    print("   □ Ensure Docker container rebuilds with new script")
    print("   □ Restart the container/service")
    
    print("\n2. 🔍 VERIFY NEW VERSION IS RUNNING:")
    print("   Look for these in the logs:")
    print("   □ '📋 Script version: v2.1-dedup-fixed-2025-09-21'")
    print("   □ '🔧 Deduplication: ENABLED (2-day window, delete-then-insert)'")
    print("   □ 'Found XXX existing cycles in last 2 days' (not 7 days)")
    print("   □ '🗑️ Attempting to delete X existing rows for update...'")
    print("   □ Uptime Kuma messages contain 'v2.1-dedup'")
    
    print("\n3. 🗄️ FIX BIGQUERY TABLE:")
    print("   The table seems to be missing. You need to:")
    print("   □ Check if table 'the-wash-pie.seko.wash-cycles' exists")
    print("   □ If missing, recreate it (run fix_bigquery_table.py)")
    print("   □ Or restore from backup if available")
    
    print("\n4. 🧪 TEST THE DEPLOYMENT:")
    print("   □ Run the script manually once")
    print("   □ Check for version info in output")
    print("   □ Verify no duplicates are created")
    print("   □ Check BigQuery for successful inserts")
    
    print("\n5. 🧹 CLEANUP REMAINING DUPLICATES:")
    print("   After confirming new version is running:")
    print("   □ Run cleanup_duplicates.py one final time")
    print("   □ Verify no duplicates remain")
    
    print("\n" + "=" * 50)
    print("🚨 CURRENT ISSUES DETECTED:")
    print("❌ Production is running OLD script version")
    print("❌ BigQuery table is missing/inaccessible")
    print("❌ Duplicates will continue until both issues are fixed")
    
    print("\n💡 QUICK FIX:")
    print("1. Redeploy with the updated script")
    print("2. Recreate the BigQuery table")
    print("3. Run cleanup script")

if __name__ == "__main__":
    print_checklist()
