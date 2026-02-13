# Athena Setup for Relay Demo

## **What is Athena?**
AWS Athena is serverless SQL for querying data in S3. No infrastructure, pay per query (~$5 per TB scanned).

---

## **Setup Steps (5 minutes)**

### **1. Open Athena Console**
https://console.aws.amazon.com/athena

### **2. Set Query Result Location (One Time)**
- Go to Settings
- Set: `s3://airbyte-poc-bucket-cb/athena-results/`
- Save

### **3. Create Database (One Time)**
```sql
CREATE DATABASE IF NOT EXISTS relay_demo;
```

### **4. Use Database**
```sql
USE relay_demo;
```

---

## **Create Tables for Demo**

### **For Synthetic Customer Data:**
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

**Test query:**
```sql
SELECT country, COUNT(*) as customers, AVG(total_spend) as avg_spend
FROM customers
WHERE is_active = true
GROUP BY country
ORDER BY customers DESC
LIMIT 10;
```

---

### **For Salesforce Opportunities:**
```sql
CREATE EXTERNAL TABLE opportunities (
    Id STRING,
    Name STRING,
    Amount DOUBLE,
    StageName STRING,
    CloseDate STRING,
    AccountId STRING,
    OwnerId STRING,
    CreatedDate STRING
)
STORED AS PARQUET
LOCATION 's3://airbyte-poc-bucket-cb/relay/demo/opportunities/';
```

**Test query:**
```sql
SELECT StageName, 
       COUNT(*) as opportunity_count,
       SUM(Amount) as total_pipeline_value,
       AVG(Amount) as avg_deal_size
FROM opportunities
GROUP BY StageName
ORDER BY total_pipeline_value DESC;
```

---

## **Demo Queries**

### **1. High-Value Deals**
```sql
SELECT Name, Amount, StageName, CloseDate
FROM opportunities
WHERE Amount > 100000
ORDER BY Amount DESC
LIMIT 20;
```

### **2. Pipeline by Stage**
```sql
SELECT StageName,
       COUNT(*) as deals,
       SUM(Amount) as value,
       AVG(Amount) as avg_size
FROM opportunities
GROUP BY StageName
ORDER BY value DESC;
```

### **3. Recent Activity**
```sql
SELECT DATE_TRUNC('month', CAST(CreatedDate AS DATE)) as month,
       COUNT(*) as new_opportunities,
       SUM(Amount) as total_value
FROM opportunities
WHERE CreatedDate >= DATE_ADD('month', -6, CURRENT_DATE)
GROUP BY DATE_TRUNC('month', CAST(CreatedDate AS DATE))
ORDER BY month DESC;
```

### **4. Owner Performance**
```sql
SELECT OwnerId,
       COUNT(*) as opportunities,
       SUM(Amount) as pipeline_value,
       AVG(Amount) as avg_deal_size
FROM opportunities
GROUP BY OwnerId
ORDER BY pipeline_value DESC
LIMIT 10;
```

---

## **Cost Analysis**

### **10M Row Customer Table:**
- File size: ~530 MB
- Query scans: ~530 MB
- Cost per query: **$0.00265** (less than a penny!)

### **Salesforce Opportunities (~1000 rows):**
- File size: ~500 KB
- Query scans: ~0.5 MB
- Cost per query: **$0.0000025** (fraction of a penny!)

**Annual cost if running 100 queries/day:**
- 100 queries × 365 days = 36,500 queries
- @ $0.00265 per query = **$97/year**

Compare to:
- Snowflake: $2,000-10,000/year
- Redshift: $5,000+/year
- BigQuery: Similar to Athena

---

## **Advantages of Athena + Relay:**

1. **No infrastructure** - Serverless
2. **Pay per query** - Not per hour/month
3. **Scales infinitely** - AWS handles it
4. **Standard SQL** - Everyone knows it
5. **BI tool compatible** - Tableau, Looker, etc.
6. **Agent-friendly** - Simple SQL generation

---

## **For the Demo:**

**Morning checklist:**
1. Open Athena console
2. Run CREATE TABLE for opportunities
3. Test with SELECT COUNT(*) - should return row count
4. Keep browser tab open for live demo
5. Rehearse typing the demo queries

**During demo:**
- After Relay syncs data
- Switch to Athena tab
- Run prepared query
- Show results in 1-2 seconds
- Mention cost (fractions of a penny)

---

## **Troubleshooting:**

**"Table not found"**
- Check LOCATION path matches S3 exactly
- Verify files exist in S3
- Try `MSCK REPAIR TABLE opportunities;`

**"No data returned"**
- Run `SELECT * FROM opportunities LIMIT 10;`
- Check if Relay wrote files to correct location
- Verify Parquet format is correct

**"Permission denied"**
- Ensure AWS credentials have S3 read access
- Check Athena query result bucket permissions

---

## **Next Steps:**

After demo, if they're interested:
1. Create table for THEIR data
2. Set up regular syncs (daily/hourly)
3. Connect BI tool (Tableau/Looker)
4. Build agent that auto-generates queries
5. Add alerts/dashboards

**Athena + Relay = Modern data stack in hours, not months.**
