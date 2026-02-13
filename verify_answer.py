import json
from collections import Counter

# Read the posts data
with open(r'C:\Users\User\.openclaw\workspace\relay\posts.json', 'r', encoding='utf-8-sig') as f:
    posts = json.load(f)

# Count posts per userId
user_post_counts = Counter(post['userId'] for post in posts)

print('\n' + '='*60)
print('FINAL ANSWER: Which users have written the most posts?')
print('='*60)
print('\nAll 10 users are tied with 10 posts each:')
print()

# Sort by post count (descending)
sorted_counts = sorted(user_post_counts.items(), key=lambda x: x[1], reverse=True)

for user_id, count in sorted_counts:
    print(f'  User {user_id:2d}: {count} posts')

print()
print(f'Total: {len(posts)} posts across {len(user_post_counts)} users')
print('='*60)

# Show sample posts from top users
print('\nSample posts from users:')
print('-'*60)
for user_id in [1, 2, 3]:
    user_posts = [p for p in posts if p['userId'] == user_id]
    print(f'\nUser {user_id} ({len(user_posts)} posts):')
    if user_posts:
        print(f'  - "{user_posts[0]["title"][:50]}..."')
