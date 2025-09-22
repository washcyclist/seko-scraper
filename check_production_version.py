#!/usr/bin/env python3
"""
Script to help verify which version of the SEKO script is running in production
"""

import os
import requests
from datetime import datetime, timezone

def check_uptime_kuma_recent_messages():
    """Check recent Uptime Kuma messages to see script version"""
    print("🔍 Checking for version indicators...")
    print("\n📋 What to look for in your monitoring:")
    print("  ✅ NEW VERSION: Messages containing 'v2.1-dedup'")
    print("  ❌ OLD VERSION: Messages without version info")
    print("  🔧 NEW VERSION: 'found XXX existing cycles in last 2 days'")
    print("  ❌ OLD VERSION: 'found XXX existing cycles in last 7 days'")
    print("  🗑️ NEW VERSION: 'Deleting X existing rows for update...'")
    print("  ❌ OLD VERSION: No delete messages")

def check_recent_bigquery_activity():
    """Instructions for checking BigQuery logs"""
    print("\n📊 To check BigQuery activity:")
    print("1. Go to BigQuery console")
    print("2. Check 'Job history' for recent queries")
    print("3. Look for:")
    print("   ✅ NEW VERSION: DELETE queries before INSERT")
    print("   ❌ OLD VERSION: Only INSERT queries")
    print("   ✅ NEW VERSION: Queries with 'INTERVAL 2 DAY'")
    print("   ❌ OLD VERSION: Queries with 'INTERVAL 7 DAY'")

def check_deployment_status():
    """Instructions for checking deployment"""
    print("\n🚀 To verify deployment:")
    print("1. Check your deployment system (Docker, cron, etc.)")
    print("2. Verify the script file timestamp matches your update")
    print("3. Check if there are multiple versions running")
    print("4. Look for container/process restart logs")
    
    print("\n🐳 If using Docker:")
    print("   docker ps | grep seko")
    print("   docker logs <container_name> | tail -20")
    
    print("\n⏰ If using cron:")
    print("   crontab -l | grep seko")
    print("   tail -f /var/log/cron")

def create_test_message():
    """Create a test message to verify the new version"""
    print("\n🧪 Test the new version:")
    print("1. Run the script manually once")
    print("2. Check the output for version info:")
    print("   '📋 Script version: v2.1-dedup-fixed-2025-09-21'")
    print("   '🔧 Deduplication: ENABLED (2-day window, delete-then-insert)'")
    print("3. Check Uptime Kuma for 'v2.1-dedup' in the message")

def main():
    print("🔍 SEKO Production Version Checker")
    print("=" * 50)
    
    check_uptime_kuma_recent_messages()
    check_recent_bigquery_activity()
    check_deployment_status()
    create_test_message()
    
    print("\n" + "=" * 50)
    print("📝 Summary:")
    print("- If you see 'v2.1-dedup' in monitoring → NEW VERSION is running")
    print("- If you see DELETE queries in BigQuery → NEW VERSION is running")
    print("- If duplicates still appear → OLD VERSION is still running")
    print("- Check your deployment process to ensure the new script is deployed")

if __name__ == "__main__":
    main()
