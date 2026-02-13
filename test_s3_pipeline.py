"""
Quick test: Verify S3 pipeline works end-to-end
"""

import requests
import time
import json

BASE_URL = "http://localhost:8001/api/v1"

print("Testing Relay S3 Pipeline End-to-End...")
print("=" * 70)

# Test 1: Create pipeline for customers
print("\n1. Creating pipeline for customers...")

pipeline_config = {
    "name": "Test E-commerce Customers",
    "source": {
        "type": "csv_url",
        "url": "http://localhost:8002/customers.csv"
    },
    "destination": {
        "type": "s3",
        "bucket": "airbyte-poc-bucket-cb",
        "path": "relay/ecommerce-test/customers"
    },
    "options": {
        "format": "parquet"
    }
}

response = requests.post(f"{BASE_URL}/pipeline/create", json=pipeline_config)
if response.status_code == 200:
    pipeline_id = response.json()["pipeline_id"]
    print(f"[OK] Pipeline created: {pipeline_id}")
else:
    print(f"[FAIL] Failed: {response.status_code}")
    print(response.text)
    exit(1)

# Test 2: Run the pipeline
print(f"\n2. Running pipeline {pipeline_id}...")

response = requests.post(f"{BASE_URL}/pipeline/{pipeline_id}/run")
if response.status_code == 200:
    run_id = response.json()["run_id"]
    print(f"✅ Pipeline started: {run_id}")
else:
    print(f"❌ Failed: {response.status_code}")
    print(response.text)
    exit(1)

# Test 3: Wait for completion
print(f"\n3. Waiting for completion...")

max_wait = 30
waited = 0
while waited < max_wait:
    response = requests.get(f"{BASE_URL}/pipeline/{pipeline_id}")
    if response.status_code == 200:
        pipeline_status = response.json()
        last_run = pipeline_status.get("runs", [{}])[-1]
        status = last_run.get("status", "unknown")
        
        if status == "success":
            print(f"✅ Pipeline completed successfully!")
            print(f"   Rows processed: {last_run.get('rows', 'N/A')}")
            print(f"   Duration: {last_run.get('duration', 'N/A')}")
            print(f"   Output: {last_run.get('output_location', 'N/A')}")
            break
        elif status == "failed":
            print(f"❌ Pipeline failed")
            print(f"   Error: {last_run.get('error', 'Unknown')}")
            exit(1)
        else:
            print(f"   Status: {status}... waiting")
            time.sleep(2)
            waited += 2
    else:
        print(f"❌ Failed to get status: {response.status_code}")
        exit(1)

if waited >= max_wait:
    print(f"❌ Timeout after {max_wait}s")
    exit(1)

print("\n" + "=" * 70)
print("✅ S3 PIPELINE TEST PASSED!")
print("=" * 70)
print("\nRelay is ready for fresh agent test with full S3 support.")
