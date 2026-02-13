"""Full test: 3-table join with the actual business question"""
import requests
import time

BASE = "http://localhost:8001/api/v1"

print("Creating 3 pipelines...")

# Customers
r1 = requests.post(f"{BASE}/pipeline/create", json={
    "name": "Final Test Customers",
    "source": {"type": "csv_url", "url": "http://localhost:8002/customers.csv"},
    "destination": {"type": "s3", "bucket": "airbyte-poc-bucket-cb", "path": "relay/final-test/customers"}
})
pipe1 = r1.json()["pipeline_id"]
requests.post(f"{BASE}/pipeline/{pipe1}/run")
print(f"1. Customers: {pipe1}")

# Orders
r2 = requests.post(f"{BASE}/pipeline/create", json={
    "name": "Final Test Orders",
    "source": {"type": "csv_url", "url": "http://localhost:8002/orders.csv"},
    "destination": {"type": "s3", "bucket": "airbyte-poc-bucket-cb", "path": "relay/final-test/orders"}
})
pipe2 = r2.json()["pipeline_id"]
requests.post(f"{BASE}/pipeline/{pipe2}/run")
print(f"2. Orders: {pipe2}")

# Items
r3 = requests.post(f"{BASE}/pipeline/create", json={
    "name": "Final Test Items",
    "source": {"type": "csv_url", "url": "http://localhost:8002/order_items.csv"},
    "destination": {"type": "s3", "bucket": "airbyte-poc-bucket-cb", "path": "relay/final-test/items"}
})
pipe3 = r3.json()["pipeline_id"]
requests.post(f"{BASE}/pipeline/{pipe3}/run")
print(f"3. Items: {pipe3}")

print("\nWaiting for all pipelines to complete...")
time.sleep(10)

# THE BUSINESS QUESTION
print("\n" + "="*70)
print("BUSINESS QUESTION:")
print("Which customers in California spent over $10,000 in 2024 on")
print("COMPLETED orders, and what was their average order value?")
print("="*70)

sql = """
SELECT 
    c.name,
    c.city,
    COUNT(DISTINCT o.order_id) as num_orders,
    SUM(i.quantity * i.price) as total_spend,
    SUM(i.quantity * i.price) / COUNT(DISTINCT o.order_id) as avg_order_value
FROM final_test_customers c
JOIN final_test_orders o ON c.customer_id = o.customer_id
JOIN final_test_items i ON o.order_id = i.order_id
WHERE 
    c.state = 'CA' 
    AND o.order_date LIKE '2024%'
    AND o.status = 'completed'
GROUP BY c.customer_id, c.name, c.city
HAVING total_spend > 10000
ORDER BY total_spend DESC
LIMIT 10
"""

print("\nExecuting query...")
r = requests.post(f"{BASE}/query", json={
    "pipelines": [pipe1, pipe2, pipe3],
    "sql": sql,
    "limit": 10
})

if r.status_code == 200:
    result = r.json()
    print(f"\nSUCCESS! Got {result['row_count']} rows in {result['execution_time_ms']}ms")
    print("\n" + "="*70)
    print("TOP 10 CALIFORNIA CUSTOMERS (>$10K IN 2024, COMPLETED ORDERS)")
    print("="*70)
    print(f"{'Name':<25} {'City':<15} {'Orders':>7} {'Total Spend':>12} {'Avg Order':>12}")
    print("-"*70)
    
    for row in result['rows']:
        print(f"{row['name']:<25} {row['city']:<15} {row['num_orders']:>7.0f} ${row['total_spend']:>11,.2f} ${row['avg_order_value']:>11,.2f}")
    
    print("="*70)
    
    # Check if top customer matches expected
    if result['rows'][0]['name'] == 'Sarah Moore':
        print("\nVALIDATION: TOP CUSTOMER MATCHES GROUND TRUTH!")
        print(f"Expected: Sarah Moore (~$30,920)")
        print(f"Got: {result['rows'][0]['name']} (${result['rows'][0]['total_spend']:,.2f})")
    else:
        print(f"\nWARNING: Expected Sarah Moore, got {result['rows'][0]['name']}")
        
else:
    print(f"\nFAILED: {r.status_code}")
    print(r.text)
