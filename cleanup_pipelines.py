"""Clean up old test pipelines"""
import requests

BASE = "http://localhost:8001/api/v1"

# Get all pipelines
r = requests.get(f"{BASE}/pipeline/list")
pipelines = r.json()["pipelines"]

print(f"Total pipelines: {len(pipelines)}\n")

# Keep only important ones
keep_keywords = ["final test", "query test", "e-commerce", "10m"]
delete_keywords = ["load", "test e", "s3 test"]

to_delete = []
for p in pipelines:
    name = p["name"].lower()
    
    # Keep if matches keep keywords
    if any(kw in name for kw in keep_keywords):
        continue
    
    # Delete if matches delete keywords or is just a test
    if any(kw in name for kw in delete_keywords) or "test" in name:
        to_delete.append((p["id"], p["name"]))

print(f"Pipelines to delete ({len(to_delete)}):")
for pid, name in to_delete[:10]:  # Show first 10
    print(f"  - {pid}: {name}")

if len(to_delete) > 10:
    print(f"  ... and {len(to_delete) - 10} more")

confirm = input(f"\nDelete {len(to_delete)} pipelines? (yes/no): ")

if confirm.lower() == "yes":
    for pid, name in to_delete:
        r = requests.delete(f"{BASE}/pipeline/{pid}")
        if r.status_code == 200:
            print(f"Deleted: {name}")
        else:
            print(f"Failed: {name}")
    
    print(f"\nDeleted {len(to_delete)} pipelines")
    
    # Show remaining
    r = requests.get(f"{BASE}/pipeline/list")
    remaining = len(r.json()["pipelines"])
    print(f"Remaining pipelines: {remaining}")
else:
    print("Cancelled")
