# Transformation Test Results

**Date:** 2026-02-06  
**Task:** Answer the business question: "Show me each user's name along with how many posts they've written. Order by most posts first."

## Discovery Process

### 1. API Exploration
Started by exploring the data API at `http://localhost:8001`:
- Discovered the `/api/v1/capabilities` endpoint which provided comprehensive API documentation
- Identified key endpoints:
  - `/api/v1/datasets/search` - Search for available datasets
  - `/api/v1/datasets/join-suggestions` - Get join key suggestions
  - `/api/v1/pipeline/create-transformation` - Create multi-dataset transformations
  - `/api/v1/metadata/{id}` - Get schema and data semantics

### 2. Dataset Discovery
Used the search endpoint to find available datasets:

```bash
curl "http://localhost:8001/api/v1/datasets/search?q=user"
```

**Results:**
- Found 2 datasets matching "user":
  1. `pipe-205e0dcc` - "RandomUser API - User Profiles"
  2. `pipe-9afbb3f3` - "JSONPlaceholder - User Posts"

Then searched for datasets with keyword "json":
```bash
curl "http://localhost:8001/api/v1/datasets/search?q=json"
```

**Results:**
- Found 2 test datasets:
  1. `pipe-bbedf4b5` - "Test Users"
  2. `pipe-51922597` - "Test Posts"

### 3. Metadata Analysis

#### Test Users Dataset (pipe-bbedf4b5)
**Source:** `http://localhost:8002/test_users.json`
**Columns:**
- `id` (int64) - Unique identifier, values 1-5
- `name` (object) - User names
- `email` (object) - Email addresses
- `city` (object) - City names

**Sample Data:**
- Alice Johnson (id=1)
- Bob Smith (id=2)
- Carol Williams (id=3)
- David Brown (id=4)
- Eve Davis (id=5)

#### Test Posts Dataset (pipe-51922597)
**Source:** `http://localhost:8002/test_posts.json`
**Columns:**
- `userId` (int64) - Foreign key to users, values 1-5
- `id` (int64) - Post identifier, values 1-50
- `title` (object) - Post title
- `body` (object) - Post content

**Statistics:**
- Total posts: 50
- Posts per user: 10 (evenly distributed)

### 4. Join Key Identification
By analyzing the metadata, I identified the relationship:
- **Join condition:** `users.id = posts.userId`
- **Join type:** LEFT JOIN (to include users even if they have no posts)
- This is a one-to-many relationship (one user has many posts)

### 5. Transformation Attempt
Attempted to use the API's transformation endpoint:
```json
{
  "name": "User Post Count Analysis",
  "sources": [
    {"type": "json_url", "url": "http://localhost:8002/test_users.json", "alias": "users"},
    {"type": "json_url", "url": "http://localhost:8002/test_posts.json", "alias": "posts"}
  ],
  "transformation": {
    "type": "join_and_aggregate",
    "join": {
      "type": "left",
      "left_dataset": "users",
      "right_dataset": "posts",
      "left_key": "id",
      "right_key": "userId"
    },
    "aggregation": {
      "group_by": ["users.id", "users.name"],
      "aggregates": [{"column": "posts.id", "function": "count", "alias": "post_count"}]
    }
  }
}
```

**Result:** API returned errors, indicating the transformation endpoint may be under development.

### 6. Manual Transformation
Implemented the transformation using Python and pandas:
1. Fetched both datasets directly from their source URLs
2. Loaded into pandas DataFrames
3. Performed groupby aggregation on posts to count by userId
4. Joined with users table to get user names
5. Sorted by post count descending

## Final Answer

```
User Name            | Post Count
---------------------|------------
Alice Johnson        | 10
Bob Smith            | 10
Carol Williams       | 10
David Brown          | 10
Eve Davis            | 10
```

## How the Data Was Combined

### Step-by-Step Transformation:

1. **Load Users Table:**
   ```
   id | name            | email                | city
   ---|-----------------|---------------------|------------
   1  | Alice Johnson   | alice@example.com   | New York
   2  | Bob Smith       | bob@example.com     | Los Angeles
   3  | Carol Williams  | carol@example.com   | Chicago
   4  | David Brown     | david@example.com   | Houston
   5  | Eve Davis       | eve@example.com     | Phoenix
   ```

2. **Load Posts Table (50 rows):**
   ```
   userId | id | title | body
   -------|----|-----------------------
   1      | 1  | "sunt aut facere..." | ...
   1      | 2  | "qui est esse..."    | ...
   ...    |... | ...                  | ...
   ```

3. **Aggregate Posts by User:**
   ```sql
   SELECT userId, COUNT(*) as post_count
   FROM posts
   GROUP BY userId
   ```
   Result:
   ```
   userId | post_count
   -------|------------
   1      | 10
   2      | 10
   3      | 10
   4      | 10
   5      | 10
   ```

4. **Join with Users:**
   ```sql
   SELECT u.name, COALESCE(p.post_count, 0) as post_count
   FROM users u
   LEFT JOIN post_counts p ON u.id = p.userId
   ORDER BY post_count DESC, u.name
   ```

## Success Criteria Checklist

✅ **Discover there are separate users and posts datasets**
   - Found via `/api/v1/datasets/search`
   - Identified "Test Users" (pipe-bbedf4b5) and "Test Posts" (pipe-51922597)

✅ **Figure out how they relate (join key)**
   - Analyzed metadata via `/api/v1/metadata/{id}`
   - Identified join: `users.id = posts.userId`

✅ **Combine them appropriately**
   - Used LEFT JOIN to preserve all users
   - Aggregated posts by userId using COUNT()

✅ **Generate the answer with user NAMES (not just IDs)**
   - Final output includes user names from the users table
   - No user IDs shown in final result

✅ **Order by post count descending**
   - Results sorted by post_count DESC
   - All users have 10 posts (tie), so alphabetical by name is secondary

## Key Learnings

1. **API Discovery Pattern:**
   - The `/capabilities` endpoint provides self-documentation
   - Search endpoint enables dataset discovery without prior knowledge
   - Metadata endpoint reveals schema and data semantics

2. **Data Relationships:**
   - Foreign key relationship between posts.userId and users.id
   - Evenly distributed data: 5 users × 10 posts each = 50 total posts

3. **Transformation Approach:**
   - API transformation endpoint appears to be under development
   - Manual transformation with pandas is reliable fallback
   - Join → Aggregate → Sort pattern successfully implemented

4. **Best Practices:**
   - Always check metadata before joining
   - Use LEFT JOIN to avoid losing users with zero posts
   - Handle NULL values in aggregations (COALESCE)

## Files Created

- `transform_data.py` - Python script performing the transformation
- `test_posts.json` - Downloaded posts dataset
- `transformation_request.json` - API transformation attempt (for reference)
- `TRANSFORMATION_TEST_RESULTS.md` - This comprehensive report
