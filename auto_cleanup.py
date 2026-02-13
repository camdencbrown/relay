"""Auto cleanup - delete test pipelines"""
import requests

BASE = "http://localhost:8001/api/v1"

# Pipelines to keep
keep_names = [
    "final test customers",
    "final test orders", 
    "final test items",
    "query test customers",
    "10m synthetic customer records",
    "e-commerce customers",
    "e-commerce orders",
    "e-commerce items"
]

r = requests.get(f"{BASE}/pipeline/list")
pipelines = r.json()["pipelines"]

deleted = 0
for p in pipelines:
    name_lower = p["name"].lower()
    
    # Keep important ones
    if any(keep in name_lower for keep in keep_names):
        print(f"KEEP: {p['name']}")
        continue
    
    # Delete everything else
    print(f"DELETE: {p['name']}")
    r = requests.delete(f"{BASE}/pipeline/{p['id']}")
    if r.status_code == 200:
        deleted += 1

print(f"\nDeleted {deleted} pipelines")

# Show final count
r = requests.get(f"{BASE}/pipeline/list")
remaining = len(r.json()["pipelines"])
print(f"Remaining: {remaining} pipelines")
