"""
Load test datasets into Relay for transformation demo
"""

import requests
import json
import time

RELAY_URL = "http://localhost:8001/api/v1"

def create_and_run_pipeline(config):
    """Create and run a pipeline"""
    # Create
    response = requests.post(f"{RELAY_URL}/pipeline/create", json=config)
    response.raise_for_status()
    result = response.json()
    pipeline_id = result["pipeline_id"]
    print(f"Created pipeline: {pipeline_id}")
    
    # Run
    response = requests.post(f"{RELAY_URL}/pipeline/{pipeline_id}/run")
    response.raise_for_status()
    run_result = response.json()
    print(f"  Status: {run_result['status']}")
    
    # Wait for completion
    run_id = run_result["run_id"]
    for i in range(30):
        response = requests.get(f"{RELAY_URL}/pipeline/{pipeline_id}/run/{run_id}")
        status = response.json()
        if status.get("status") in ["success", "failed"]:
            print(f"  Final: {status['status']}")
            if status.get("rows_processed"):
                print(f"  Rows: {status['rows_processed']}")
            break
        time.sleep(0.5)
    
    return pipeline_id

# Pipeline 1: Users
print("\n=== Loading Users Dataset ===")
users_pipeline = {
    "name": "Test Users",
    "source": {
        "type": "json_url",
        "url": "http://localhost:8002/test_users.json"
    },
    "destination": {
        "type": "s3",
        "bucket": "airbyte-poc-bucket-cb",
        "path": "relay/test-transform/users/"
    }
}

users_id = create_and_run_pipeline(users_pipeline)

# Pipeline 2: Posts
print("\n=== Loading Posts Dataset ===")
posts_pipeline = {
    "name": "Test Posts",
    "source": {
        "type": "json_url",
        "url": "http://localhost:8002/test_posts.json"
    },
    "destination": {
        "type": "s3",
        "bucket": "airbyte-poc-bucket-cb",
        "path": "relay/test-transform/posts/"
    }
}

posts_id = create_and_run_pipeline(posts_pipeline)

print("\n=== SUCCESS ===")
print(f"\nUsers Pipeline: {users_id}")
print(f"Posts Pipeline: {posts_id}")
print("\nDatasets loaded! Ready for transformation test.")
