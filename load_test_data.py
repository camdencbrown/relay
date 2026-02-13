"""
Load test datasets for transformation demo
"""

import requests
import json

RELAY_URL = "http://localhost:8001/api/v1"

# Test data sources
SOURCES = [
    {
        "name": "RandomUser API - User Profiles",
        "source": {
            "type": "rest_api",
            "url": "https://randomuser.me/api/?results=50&seed=relay"
        },
        "destination": {
            "type": "s3",
            "bucket": "airbyte-poc-bucket-cb",
            "path": "relay/test-data/users/"
        }
    },
    {
        "name": "JSONPlaceholder - User Posts",
        "source": {
            "type": "rest_api",
            "url": "https://jsonplaceholder.typicode.com/posts"
        },
        "destination": {
            "type": "s3",
            "bucket": "airbyte-poc-bucket-cb",
            "path": "relay/test-data/posts/"
        }
    }
]

def create_pipeline(config):
    """Create a pipeline in Relay"""
    response = requests.post(
        f"{RELAY_URL}/pipeline/create",
        json=config
    )
    response.raise_for_status()
    return response.json()

def run_pipeline(pipeline_id):
    """Run a pipeline"""
    response = requests.post(
        f"{RELAY_URL}/pipeline/{pipeline_id}/run"
    )
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    print("Loading test datasets into Relay...\n")
    
    for source_config in SOURCES:
        print(f"Creating: {source_config['name']}")
        
        # Create pipeline
        result = create_pipeline(source_config)
        pipeline_id = result["pipeline_id"]
        print(f"  Pipeline ID: {pipeline_id}")
        
        # Run pipeline
        run_result = run_pipeline(pipeline_id)
        print(f"  Status: {run_result['status']}")
        print()
    
    print("✅ Test data loaded successfully!")
    print("\nDatasets available:")
    print("1. RandomUser API - User Profiles (50 users)")
    print("2. JSONPlaceholder - User Posts (100 posts)")
    print("\nAgent can now search and join these datasets.")
