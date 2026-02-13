"""Test the /export endpoint"""
import requests

BASE = "http://localhost:8001/api/v1"

# Get some pipelines to query
r = requests.get(f"{BASE}/pipeline/list")
pipes = r.json()["pipelines"]
final_test = [p["id"] for p in pipes if "final test" in p["name"].lower()][:3]

if len(final_test) < 3:
    print("Need Final Test pipelines. Using any 3 pipelines...")
    final_test = [p["id"] for p in pipes[:3]]

print(f"Using pipelines: {final_test}")

# Test 1: CSV Export
print("\n1. Testing CSV export...")
sql = """
SELECT c.name, c.state, COUNT(DISTINCT o.order_id) as orders
FROM final_test_customers c
JOIN final_test_orders o ON c.customer_id = o.customer_id
WHERE c.state = 'CA'
GROUP BY c.customer_id, c.name, c.state
ORDER BY orders DESC
LIMIT 10
"""

r = requests.post(f"{BASE}/export", json={
    "pipelines": final_test,
    "sql": sql,
    "format": "csv",
    "filename": "top_ca_customers.csv"
})

if r.status_code == 200:
    print(f"✓ CSV export: {len(r.content)} bytes")
    print(f"  Headers: {dict(r.headers)}")
    print(f"  First 200 chars:\n{r.text[:200]}")
else:
    print(f"✗ Failed: {r.status_code}")
    print(r.text)

# Test 2: JSON Export
print("\n2. Testing JSON export...")
r = requests.post(f"{BASE}/export", json={
    "pipelines": final_test,
    "sql": sql,
    "format": "json"
})

if r.status_code == 200:
    print(f"✓ JSON export: {len(r.content)} bytes")
    import json
    data = json.loads(r.content)
    print(f"  Rows: {len(data)}")
    print(f"  Sample: {data[0] if data else 'empty'}")
else:
    print(f"✗ Failed: {r.status_code}")
    print(r.text)

# Test 3: Excel Export
print("\n3. Testing Excel export...")
r = requests.post(f"{BASE}/export", json={
    "pipelines": final_test,
    "sql": sql,
    "format": "excel",
    "filename": "top_customers.xlsx"
})

if r.status_code == 200:
    print(f"✓ Excel export: {len(r.content)} bytes")
    with open("test_export.xlsx", "wb") as f:
        f.write(r.content)
    print(f"  Saved to: test_export.xlsx")
else:
    print(f"✗ Failed: {r.status_code}")
    print(r.text)

print("\n✓ Export endpoint test complete!")
