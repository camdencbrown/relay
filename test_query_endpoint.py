"""Test the new query endpoint"""
import requests
import json

BASE = "http://localhost:8001/api/v1"

print("Testing Query Endpoint...")
print("=" * 70)

# 1. List pipelines
print("\n1. Getting available pipelines...")
r = requests.get(f"{BASE}/pipeline/list")
pipelines = r.json()["pipelines"]

# Filter for our e-commerce test pipelines
ecommerce_pipes = [p for p in pipelines if "customer" in p["name"].lower() or "order" in p["name"].lower() or "item" in p["name"].lower()]

print(f"Found {len(ecommerce_pipes)} e-commerce pipelines:")
for p in ecommerce_pipes[:5]:
    print(f"  - {p['id']}: {p['name']}")

if len(ecommerce_pipes) < 3:
    print("\nNeed to create pipelines first. Creating now...")
    
    # Create customers pipeline
    r1 = requests.post(f"{BASE}/pipeline/create", json={
        "name": "E-commerce Customers",
        "source": {"type": "csv_url", "url": "http://localhost:8002/customers.csv"},
        "destination": {"type": "s3", "bucket": "airbyte-poc-bucket-cb", "path": "relay/query-test/customers"}
    })
    pipe1 = r1.json()["pipeline_id"]
    requests.post(f"{BASE}/pipeline/{pipe1}/run")
    
    # Create orders pipeline  
    r2 = requests.post(f"{BASE}/pipeline/create", json={
        "name": "E-commerce Orders",
        "source": {"type": "csv_url", "url": "http://localhost:8002/orders.csv"},
        "destination": {"type": "s3", "bucket": "airbyte-poc-bucket-cb", "path": "relay/query-test/orders"}
    })
    pipe2 = r2.json()["pipeline_id"]
    requests.post(f"{BASE}/pipeline/{pipe2}/run")
    
    # Create items pipeline
    r3 = requests.post(f"{BASE}/pipeline/create", json={
        "name": "E-commerce Items",
        "source": {"type": "csv_url", "url": "http://localhost:8002/order_items.csv"},
        "destination": {"type": "s3", "bucket": "airbyte-poc-bucket-cb", "path": "relay/query-test/items"}
    })
    pipe3 = r3.json()["pipeline_id"]
    requests.post(f"{BASE}/pipeline/{pipe3}/run")
    
    print(f"Created pipelines: {pipe1}, {pipe2}, {pipe3}")
    print("Waiting 5 seconds for completion...")
    import time
    time.sleep(5)
    
    ecommerce_pipes = [
        {"id": pipe1, "name": "E-commerce Customers"},
        {"id": pipe2, "name": "E-commerce Orders"},
        {"id": pipe3, "name": "E-commerce Items"}
    ]

# Get the pipeline IDs
pipe_ids = [p["id"] for p in ecommerce_pipes[:3]]

# 2. Get schema
print(f"\n2. Getting schema for {len(pipe_ids)} pipelines...")
r = requests.post(f"{BASE}/schema", json={"pipelines": pipe_ids})
if r.status_code == 200:
    schemas = r.json()["schemas"]
    print(f"Available tables:")
    for pid, schema in schemas.items():
        print(f"  - {schema['table_alias']} ({len(schema.get('columns', []))} columns)")
else:
    print(f"Schema request failed: {r.status_code}")
    print(r.text)

# 3. Execute simple query
print(f"\n3. Testing simple query (top 10 customers by state)...")
sql = """
SELECT state, COUNT(*) as customer_count
FROM e_commerce_customers
GROUP BY state
ORDER BY customer_count DESC
LIMIT 10
"""

r = requests.post(f"{BASE}/query", json={
    "pipelines": [pipe_ids[0]],
    "sql": sql,
    "limit": 10
})

if r.status_code == 200:
    result = r.json()
    print(f"Success! Got {result['row_count']} rows in {result['execution_time_ms']}ms")
    print("Results:")
    for row in result['rows'][:5]:
        print(f"  {row}")
else:
    print(f"Query failed: {r.status_code}")
    print(r.text)

# 4. Execute complex multi-table query
print(f"\n4. Testing complex query (CA customers with completed orders in 2024)...")
sql = """
SELECT 
    c.name,
    COUNT(DISTINCT o.order_id) as order_count,
    SUM(i.quantity * i.price) as total_spend
FROM e_commerce_customers c
JOIN e_commerce_orders o ON c.customer_id = o.customer_id
JOIN e_commerce_items i ON o.order_id = i.order_id
WHERE c.state = 'CA' 
    AND o.order_date LIKE '2024%'
    AND o.status = 'completed'
GROUP BY c.customer_id, c.name
HAVING total_spend > 10000
ORDER BY total_spend DESC
LIMIT 10
"""

r = requests.post(f"{BASE}/query", json={
    "pipelines": pipe_ids,
    "sql": sql,
    "limit": 10
})

if r.status_code == 200:
    result = r.json()
    print(f"Success! Got {result['row_count']} rows in {result['execution_time_ms']}ms")
    print("\nTop 10 CA customers (>$10K in 2024):")
    print("-" * 70)
    for row in result['rows']:
        print(f"{row['name']:25s} | Orders: {row['order_count']:2d} | Total: ${row['total_spend']:,.2f}")
    print("-" * 70)
else:
    print(f"Query failed: {r.status_code}")
    print(r.text)

print("\n" + "=" * 70)
print("Query endpoint test complete!")
