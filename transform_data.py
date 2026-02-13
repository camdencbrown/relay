import json
import pandas as pd

# Load the data
with open('test_posts.json', 'r') as f:
    posts = json.load(f)

users = [
    {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "city": "New York"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "city": "Los Angeles"},
    {"id": 3, "name": "Carol Williams", "email": "carol@example.com", "city": "Chicago"},
    {"id": 4, "name": "David Brown", "email": "david@example.com", "city": "Houston"},
    {"id": 5, "name": "Eve Davis", "email": "eve@example.com", "city": "Phoenix"}
]

# Convert to DataFrames
users_df = pd.DataFrame(users)
posts_df = pd.DataFrame(posts)

print("Users DataFrame:")
print(users_df)
print(f"\nTotal users: {len(users_df)}")

print("\nPosts DataFrame:")
print(posts_df.head())
print(f"\nTotal posts: {len(posts_df)}")

# Count posts per user
post_counts = posts_df.groupby('userId').size().reset_index(name='post_count')
print("\nPost counts by userId:")
print(post_counts)

# Join with users to get names
result = users_df.merge(post_counts, left_on='id', right_on='userId', how='left')
result['post_count'] = result['post_count'].fillna(0).astype(int)

# Select and sort
result = result[['name', 'post_count']].sort_values('post_count', ascending=False)

print("\n" + "="*50)
print("FINAL ANSWER:")
print("="*50)
print(f"\n{'User Name':<20} | {'Post Count':<10}")
print("-" * 35)
for _, row in result.iterrows():
    print(f"{row['name']:<20} | {row['post_count']:<10}")
