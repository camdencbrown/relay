"""
Create clean test datasets for transformation demo
"""

import requests
import json

RELAY_URL = "http://localhost:8001/api/v1"

# Create simple users dataset
users_data = [
    {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "city": "New York"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "city": "Los Angeles"},
    {"id": 3, "name": "Carol Williams", "email": "carol@example.com", "city": "Chicago"},
    {"id": 4, "name": "David Brown", "email": "david@example.com", "city": "Houston"},
    {"id": 5, "name": "Eve Davis", "email": "eve@example.com", "city": "Phoenix"},
]

# Posts will come from JSONPlaceholder (already has userId 1-10)
# We'll filter to just userId 1-5 to match our users

print("Creating test datasets...")
print("\n1. Users Dataset")
print(f"   {len(users_data)} users (id 1-5)")

# Save users to a local JSON file that we can serve
with open("test_users.json", "w") as f:
    json.dump(users_data, f, indent=2)

print("   [OK] Saved to test_users.json")

print("\n2. Posts Dataset")
print("   Using JSONPlaceholder API (will filter to userId 1-5)")

# Get posts from JSONPlaceholder
posts = requests.get("https://jsonplaceholder.typicode.com/posts").json()
# Filter to just users 1-5
filtered_posts = [p for p in posts if p["userId"] <= 5]

print(f"   [OK] {len(filtered_posts)} posts from 5 users")

# Save filtered posts
with open("test_posts.json", "w") as f:
    json.dump(filtered_posts, f, indent=2)

print("\nTest datasets ready!")
print("\nNext: Load these into Relay as pipelines")
