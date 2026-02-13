import requests
import json

RELAY_URL = "http://localhost:8001/api/v1"

# Simple transformation test
config = {
    "name": "Users with Post Count",
    "sources": [
        {
            "type": "json_url",
            "url": "http://localhost:8002/test_users.json",
            "alias": "users"
        },
        {
            "type": "json_url", 
            "url": "http://localhost:8002/test_posts.json",
            "alias": "posts"
        }
    ],
    "join": {
        "left": "users",
        "right": "posts",
        "on": "users.id = posts.userId",
        "how": "left"
    },
    "aggregate": {
        "group_by": ["name"],
        "metrics": {
            "post_count": "COUNT(id)"
        }
    },
    "destination": {
        "type": "s3",
        "bucket": "airbyte-poc-bucket-cb",
        "path": "relay/test-transform/result/"
    }
}

print("Testing transformation API...")
print(json.dumps(config, indent=2))
print("\nSending request...")

try:
    response = requests.post(
        f"{RELAY_URL}/pipeline/create-transformation",
        json=config,
        timeout=30
    )
    
    print(f"\nStatus: {response.status_code}")
    print(f"Response: {response.text}")
    
    if response.status_code == 200:
        result = response.json()
        print("\nSUCCESS!")
        print(json.dumps(result, indent=2))
    else:
        print("\nERROR!")
        
except Exception as e:
    print(f"\nException: {e}")
