# Testing Complex Source → Complex Destination

## What We Can Test

### Option 1: MySQL → Postgres (Most Realistic)
**What you need:**
- MySQL database (local or cloud)
- Postgres database (local or cloud)
- Credentials for both

**Setup Options:**
1. **Docker (Easiest):**
   ```bash
   # Start MySQL
   docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=testdb mysql:8
   
   # Start Postgres
   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password -e POSTGRES_DB=testdb postgres:16
   ```

2. **Cloud (Free Tiers):**
   - MySQL: PlanetScale, Railway, or Amazon RDS Free Tier
   - Postgres: Supabase, Railway, or Amazon RDS Free Tier

### Option 2: REST API → S3 (Already Working!)
**What you need:**
- Any public REST API
- S3 bucket (we have this)

**Example APIs to test:**
- JSONPlaceholder: https://jsonplaceholder.typicode.com/users
- GitHub API: https://api.github.com/repos/openclaw/openclaw
- Random User API: https://randomuser.me/api/?results=100

### Option 3: Postgres (Airbyte) → S3
**What you need:**
- We already have Airbyte's internal Postgres running
- S3 bucket (we have this)

**How to test:**
1. Query Airbyte's internal Postgres
2. Move data to S3 via Relay

This proves Relay can work WITH Airbyte!

---

## Recommended: Test REST API → S3 First

**Why:** No setup needed, proves complex source handling

### Create Pipeline via API:

```bash
curl -X POST http://localhost:8001/api/v1/pipeline/create \
  -H "Content-Type: application/json" \
  -d '{
    "name": "GitHub Stars to S3",
    "description": "Fetch GitHub repo data and store in S3",
    "source": {
      "type": "rest_api",
      "url": "https://api.github.com/repos/openclaw/openclaw",
      "method": "GET",
      "headers": {
        "User-Agent": "Relay-Test"
      }
    },
    "destination": {
      "type": "s3",
      "bucket": "airbyte-poc-bucket-cb",
      "path": "relay/github/"
    },
    "schedule": {
      "enabled": true,
      "interval": "daily"
    }
  }'
```

Then run it:
```bash
curl -X POST http://localhost:8001/api/v1/pipeline/{pipeline-id}/run
```

---

## If You Want to Test MySQL → Postgres

### Quick Docker Setup:

```powershell
# 1. Start MySQL
docker run -d --name relay-mysql `
  -p 3306:3306 `
  -e MYSQL_ROOT_PASSWORD=password `
  -e MYSQL_DATABASE=testdb `
  mysql:8

# Wait 10 seconds for MySQL to start
Start-Sleep -Seconds 10

# 2. Create test data in MySQL
docker exec -i relay-mysql mysql -uroot -ppassword testdb <<EOF
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at DATETIME
);

INSERT INTO users VALUES
(1, 'Alice', 'alice@example.com', NOW()),
(2, 'Bob', 'bob@example.com', NOW()),
(3, 'Charlie', 'charlie@example.com', NOW());
EOF

# 3. Start Postgres
docker run -d --name relay-postgres `
  -p 5432:5432 `
  -e POSTGRES_PASSWORD=password `
  -e POSTGRES_DB=testdb `
  postgres:16

# Wait 5 seconds for Postgres to start
Start-Sleep -Seconds 5

# 4. Create destination table
docker exec -i relay-postgres psql -U postgres -d testdb <<EOF
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP
);
EOF
```

### Then Create Relay Pipeline:

```json
{
  "name": "MySQL to Postgres Sync",
  "source": {
    "type": "mysql",
    "host": "localhost",
    "port": 3306,
    "database": "testdb",
    "username": "root",
    "password": "password",
    "query": "SELECT * FROM users"
  },
  "destination": {
    "type": "postgres",
    "host": "localhost",
    "port": 5432,
    "database": "testdb",
    "username": "postgres",
    "password": "password",
    "table": "users",
    "if_exists": "replace"
  },
  "schedule": {
    "enabled": true,
    "interval": "hourly"
  }
}
```

---

## My Recommendation

**Start with REST API → S3** because:
1. No setup needed
2. Proves Relay handles complex sources
3. We can test RIGHT NOW
4. Shows real-world use case (API data to data lake)

**Then add MySQL/Postgres if you want to show database-to-database migration.**

Want me to create a test pipeline with a real REST API right now?
