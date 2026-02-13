"""
Test Relay with 10 million rows of synthetic data
Proves streaming + parallel processing at scale
"""

import requests
import json
from datetime import datetime

# Relay API
BASE_URL = "http://localhost:8001/api/v1"

print("=" * 60)
print("RELAY SCALE TEST: 10 MILLION ROWS")
print("=" * 60)
print()

# Step 1: Create synthetic data source
print("Creating pipeline with synthetic data source...")
print()

pipeline_request = {
    "name": "10M Synthetic Customer Records",
    "description": "Stress test with 10 million customer records",
    "source": {
        "type": "synthetic",
        "row_count": 10000000,  # 10 million
        "schema": {
            "customer_id": "uuid",
            "email": "email",
            "first_name": "first_name",
            "last_name": "last_name",
            "signup_date": "date",
            "total_spend": "currency",
            "is_active": "boolean",
            "country": "country",
            "age": "integer:18:80",
            "loyalty_points": "integer:0:10000"
        }
    },
    "destination": {
        "type": "s3",
        "bucket": "airbyte-poc-bucket-cb",
        "path": "relay/synthetic/10m-customers/"
    },
    "options": {
        "format": "parquet",
        "compression": "snappy",
        "streaming": True,
        "parallel": True,
        "combine_chunks": False,  # Keep as separate files for speed
        "generate_metadata": True,
        "ai_semantics": True
    }
}

print(f"  Source: Synthetic data generator")
print(f"  Rows: 10,000,000")
print(f"  Destination: S3 (Parquet + Snappy)")
print(f"  Streaming: ENABLED")
print(f"  Parallel: ENABLED")
print()

response = requests.post(f"{BASE_URL}/pipeline/create", json=pipeline_request)
result = response.json()

if result.get("status") == "created":
    pipeline_id = result["pipeline_id"]
    print(f"SUCCESS! Pipeline created: {pipeline_id}")
    print()
    
    # Step 2: Run the pipeline
    print("Starting pipeline execution...")
    print("   This will take ~15-20 minutes for 10M rows")
    print("   Watch the progress below:")
    print()
    
    run_response = requests.post(f"{BASE_URL}/pipeline/{pipeline_id}/run")
    run_result = run_response.json()
    
    if run_result.get("status") == "started":
        run_id = run_result["run_id"]
        print(f"   Run ID: {run_id}")
        print(f"   Started: {run_result['started_at']}")
        print()
        print("   Polling for status every 30 seconds...")
        print("   " + "-" * 50)
        
        import time
        
        last_progress = ""
        start_time = datetime.now()
        
        while True:
            time.sleep(30)  # Poll every 30 seconds
            
            status_response = requests.get(f"{BASE_URL}/pipeline/{pipeline_id}/run/{run_id}")
            status = status_response.json()
            
            current_progress = status.get("progress", "")
            current_status = status.get("status", "")
            
            if current_progress != last_progress:
                elapsed = (datetime.now() - start_time).total_seconds()
                print(f"   [{elapsed:.0f}s] {current_progress}")
                last_progress = current_progress
            
            if current_status in ["success", "failed"]:
                print("   " + "-" * 50)
                print()
                break
        
        # Step 3: Show results
        if status["status"] == "success":
            duration = status.get("duration_seconds", 0)
            rows = status.get("rows_processed", 0)
            chunks = status.get("chunks_processed", 0)
            files = status.get("files_written", [])
            
            print("=" * 60)
            print("SUCCESS!")
            print("=" * 60)
            print()
            print(f"Results:")
            print(f"   Rows processed: {rows:,}")
            print(f"   Chunks: {chunks:,}")
            print(f"   Files written: {len(files)}")
            print(f"   Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
            print(f"   Throughput: {rows/duration:,.0f} rows/second")
            print()
            print(f"Output location:")
            print(f"   {status.get('output_file', 'N/A')}")
            print()
            
            if status.get("metadata_generated"):
                print(f"Metadata:")
                print(f"   Generated: Yes")
                print(f"   Columns needing review: {status.get('columns_needing_review', 0)}")
                print(f"   View at: http://localhost:8001/metadata")
                print()
            
            # Calculate performance metrics
            rows_per_min = rows / (duration / 60)
            print(f"Performance:")
            print(f"   {rows_per_min:,.0f} rows/minute")
            print(f"   {rows_per_min/60:,.0f} rows/second")
            print()
            
            # Extrapolate to larger datasets
            print(f"Extrapolation:")
            for target_rows in [50_000_000, 100_000_000, 500_000_000]:
                estimated_time = (target_rows / rows) * duration
                print(f"   {target_rows:,} rows -> ~{estimated_time/60:.0f} minutes")
            print()
            
        else:
            print("=" * 60)
            print("FAILED")
            print("=" * 60)
            print()
            print(f"Error: {status.get('error', 'Unknown')}")
            print()
            if "traceback" in status:
                print("Traceback:")
                print(status["traceback"])
    
    else:
        print(f"Failed to start pipeline: {run_result}")
        
else:
    print(f"Failed to create pipeline: {result}")
