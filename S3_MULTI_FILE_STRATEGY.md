# S3 Multi-File Strategy
## How to Work with 1000s of Files

## **The "Problem" (Actually a Feature!)**

Your 10M row dataset is split into 1,000 files:
- `chunk_000000.parquet`
- `chunk_000001.parquet`
- ...
- `chunk_000999.parquet`

This seems messy, but it's actually **optimal** for cloud-native data systems.

---

## **Why Multiple Files is GOOD:**

### 1. **Parallelism**
Query engines (Athena, Snowflake, BigQuery) read multiple files in parallel:
- 1 big file: 1 reader = slow
- 1000 small files: 1000 parallel readers = **100x faster**

### 2. **Incremental Updates**
Add new data without rewriting entire dataset:
- Append new chunks daily
- No need to merge files
- Queries automatically include new files

### 3. **Fault Tolerance**
If one file corrupts:
- Lose 10,000 rows (0.1%)
- Not entire dataset
- Can reprocess just that chunk

### 4. **Cost Efficiency**
S3 SELECT and Athena:
- Only scan files that match query predicates
- Skip 90% of data → pay 10% of cost

---

## **How to Query It:**

### **Option 1: AWS Athena (Serverless SQL)**

**Setup (one time):**
```sql
CREATE EXTERNAL TABLE customers (
    customer_id STRING,
    email STRING,
    first_name STRING,
    last_name STRING,
    signup_date DATE,
    total_spend DOUBLE,
    is_active BOOLEAN,
    country STRING,
    age INT,
    loyalty_points INT
)
STORED AS PARQUET
LOCATION 's3://airbyte-poc-bucket-cb/relay/synthetic/10m-customers/';
```

**Query (like normal SQL):**
```sql
-- Athena automatically reads all 1000 files
SELECT country, AVG(total_spend) as avg_spend
FROM customers
WHERE is_active = true
GROUP BY country
ORDER BY avg_spend DESC;

-- Only scans relevant chunks (partitions)
SELECT * FROM customers WHERE signup_date > '2025-01-01';
```

**Cost:** ~$5 per TB scanned (10M rows = ~530 MB = **$0.003 per query**)

---

### **Option 2: DuckDB (Local SQL)**

**Read all files as one table:**
```python
import duckdb

# DuckDB automatically handles multiple files
df = duckdb.query("""
    SELECT * FROM 's3://airbyte-poc-bucket-cb/relay/synthetic/10m-customers/*.parquet'
    WHERE country = 'USA'
""").to_df()
```

**Cost:** Free, runs on your laptop

---

### **Option 3: Pandas (for small queries)**

```python
import pandas as pd
import boto3

# Read just one chunk for sampling
df = pd.read_parquet('s3://bucket/path/chunk_000000.parquet')

# Or read all (only if you need ALL data)
df = pd.read_parquet('s3://bucket/path/', engine='pyarrow')
# ^ PyArrow automatically reads all files
```

---

### **Option 4: Snowflake**

```sql
-- Create external stage
CREATE STAGE relay_data
URL = 's3://airbyte-poc-bucket-cb/relay/synthetic/10m-customers/'
CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...');

-- Copy into Snowflake table (one time)
COPY INTO customers FROM @relay_data
FILE_FORMAT = (TYPE = PARQUET);

-- Now query like any table
SELECT * FROM customers WHERE age > 50;
```

---

## **Semantic Layer Integration:**

### **The Vision:**
Agent asks: "Show me top spending customers by country"

**Without semantic layer:**
- Agent doesn't know table exists
- Doesn't know column names
- Doesn't know what "total_spend" means

**With semantic layer (Relay's metadata):**

**1. Agent queries Relay metadata:**
```json
GET /api/v1/metadata/pipe-4752f81e
{
  "columns": [
    {
      "name": "total_spend",
      "semantic_type": "currency",
      "description": "Total customer spend in USD",
      "business_meaning": "Lifetime revenue from customer"
    },
    {
      "name": "country",
      "semantic_type": "text",
      "description": "Customer's country of residence"
    }
  ],
  "location": "s3://airbyte-poc-bucket-cb/relay/synthetic/10m-customers/",
  "format": "parquet"
}
```

**2. Agent generates query:**
```sql
SELECT country, SUM(total_spend) as revenue
FROM 's3://airbyte-poc-bucket-cb/relay/synthetic/10m-customers/*.parquet'
GROUP BY country
ORDER BY revenue DESC
LIMIT 10;
```

**3. Agent understands results:**
- Knows "revenue" is in USD
- Can explain business meaning
- Suggests follow-up questions

---

## **Best Practice: Add Partitioning**

### **Problem:**
1000 files in one folder - Athena scans all even if you only need recent data.

### **Solution: Partition by date**

**Relay creates structure:**
```
s3://bucket/customers/
  year=2023/
    month=01/
      chunk_000000.parquet
      chunk_000001.parquet
  year=2023/
    month=02/
      ...
  year=2024/
    ...
```

**Query:**
```sql
SELECT * FROM customers
WHERE year = 2024 AND month = 01;
-- Only scans Jan 2024 folder, skips 11 other months
```

**Cost savings:** Query 1/12 of data = 1/12 of cost

---

## **Relay V2 Features (Roadmap):**

### 1. **Automatic Partitioning**
```json
{
  "options": {
    "partition_by": "signup_date",
    "partition_format": "year=%Y/month=%m/day=%d"
  }
}
```

### 2. **File Consolidation**
```json
{
  "options": {
    "consolidate": {
      "max_files_per_partition": 10,
      "min_file_size": "100MB"
    }
  }
}
```

### 3. **Built-in Query Interface**
```
GET /api/v1/query/{pipeline_id}?sql=SELECT * FROM data WHERE country='USA'
```
Relay translates to S3 path, executes via DuckDB, returns results.

### 4. **Athena Integration**
```json
{
  "destination": {
    "type": "athena",
    "database": "analytics",
    "table": "customers",
    "auto_create": true
  }
}
```
Relay automatically:
- Creates Athena table
- Manages schema
- Handles file organization
- Updates metadata

---

## **Your Presentation Talking Points:**

**Colleague:** "But isn't 1000 files messy?"

**You:** 
> "Actually, that's how cloud-native systems work. Athena and Snowflake are designed for this. They read files in parallel - 1000 files means 1000 parallel readers, which is 100x faster than one giant file. Plus, we only pay to scan what we query."

**Colleague:** "How do we query it?"

**You:**
> "Three ways: 
> 1. AWS Athena for serverless SQL ($0.003 per query for this dataset)
> 2. DuckDB locally for free
> 3. Load into Snowflake/Redshift once if needed
> 
> The semantic layer tells agents exactly where the data is and what it means, so they can auto-generate queries."

**Colleague:** "What about visualization?"

**You:**
> "Any BI tool works: Tableau, Looker, Metabase all connect to Athena/Snowflake. Or the agent can query it and generate charts with matplotlib/plotly. The metadata layer ensures the agent understands what to visualize."

---

## **Demo Script:**

**1. Show the files in S3:**
```bash
aws s3 ls s3://bucket/path/ --human-readable
# 1000 files, 530 MB total
```

**2. Query with DuckDB (instant):**
```python
import duckdb
df = duckdb.query("""
    SELECT country, COUNT(*) as customers, AVG(total_spend) as avg
    FROM 's3://bucket/path/*.parquet'
    GROUP BY country
    ORDER BY customers DESC
    LIMIT 5
""").to_df()
print(df)
```

**3. Show Athena (if time):**
- Create table in console
- Run query
- Show $0.003 cost

**4. Show metadata:**
- Navigate to Relay UI
- Show semantic layer
- Explain how agent uses it

---

## **Bottom Line:**

Multiple files in S3 is **not a problem** - it's the **industry standard** for cloud-native data.

The semantic layer bridges the gap between:
- Raw files in S3 (technical)
- Business meaning (human/agent understanding)
- Query generation (agent capability)

**This is what makes Relay different.**
