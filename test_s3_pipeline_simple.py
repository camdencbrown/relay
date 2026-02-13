"""Quick test: Verify S3 pipeline works"""
import requests, time

BASE = "http://localhost:8001/api/v1"

print("Testing S3 Pipeline...")

# Create
r = requests.post(f"{BASE}/pipeline/create", json={
    "name": "Test Customers S3",
    "source": {"type": "csv_url", "url": "http://localhost:8002/customers.csv"},
    "destination": {"type": "s3", "bucket": "airbyte-poc-bucket-cb", "path": "relay/test/customers"},
    "options": {"format": "parquet"}
})
print(f"Create: {r.status_code}")
pid = r.json()["pipeline_id"]
print(f"Pipeline ID: {pid}")

# Run
r = requests.post(f"{BASE}/pipeline/{pid}/run")
print(f"Run: {r.status_code}")

# Wait
for i in range(15):
    r = requests.get(f"{BASE}/pipeline/{pid}")
    runs = r.json().get("runs", [])
    if runs:
        status = runs[-1]["status"]
        print(f"Status: {status}")
        if status == "success":
            print(f"Rows: {runs[-1]['rows']}")
            print(f"Output: {runs[-1]['output_location']}")
            print("SUCCESS!")
            break
        elif status == "failed":
            print(f"Error: {runs[-1].get('error')}")
            exit(1)
    time.sleep(2)
