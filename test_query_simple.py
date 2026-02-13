"""Simple test of query endpoint"""
import requests
import time

BASE = "http://localhost:8001/api/v1"

print("Creating test pipelines...")

# Create customers
r = requests.post(f"{BASE}/pipeline/create", json={
    "name": "Query Test Customers",
    "source": {"type": "csv_url", "url": "http://localhost:8002/customers.csv"},
    "destination": {"type": "s3", "bucket": "airbyte-poc-bucket-cb", "path": "relay/query-test/customers"}
})
pipe1 = r.json()["pipeline_id"]
print(f"Created {pipe1}")

# Run it
requests.post(f"{BASE}/pipeline/{pipe1}/run")
print("Waiting for completion...")
time.sleep(3)

# Query it
print("\nQuerying data...")
sql = "SELECT state, COUNT(*) as count FROM query_test_customers GROUP BY state ORDER BY count DESC LIMIT 5"

r = requests.post(f"{BASE}/query", json={
    "pipelines": [pipe1],
    "sql": sql
})

if r.status_code == 200:
    result = r.json()
    print(f"SUCCESS! Got {result['row_count']} rows in {result['execution_time_ms']}ms")
    print("\nResults:")
    for row in result['rows']:
        print(f"  {row['state']}: {row['count']} customers")
else:
    print(f"FAILED: {r.status_code}")
    print(r.text)
