"""
Demo: Query Relay data with DuckDB
Shows how to analyze data directly from S3 without loading
"""

import duckdb
import os
from dotenv import load_dotenv

# Load AWS credentials from .env file
load_dotenv()

# Verify credentials are loaded (will use from .env or environment)
if not os.getenv('AWS_ACCESS_KEY_ID'):
    print("ERROR: AWS credentials not found!")
    print("Please configure .env file (see .env.example)")
    exit(1)

print("=" * 60)
print("RELAY + DUCKDB DEMO")
print("Querying 10M rows directly from S3")
print("=" * 60)
print()

# Query 1: Customer counts by country
print("Query 1: Top 10 countries by customer count")
print("-" * 60)

result = duckdb.query("""
    SELECT country, 
           COUNT(*) as customer_count,
           AVG(total_spend) as avg_spend,
           SUM(total_spend) as total_revenue
    FROM 's3://airbyte-poc-bucket-cb/relay/synthetic/10m-customers/*.parquet'
    GROUP BY country
    ORDER BY customer_count DESC
    LIMIT 10
""").to_df()

print(result.to_string())
print()

# Query 2: Active vs Inactive customers
print("\nQuery 2: Active vs Inactive Analysis")
print("-" * 60)

result = duckdb.query("""
    SELECT is_active,
           COUNT(*) as customers,
           AVG(total_spend) as avg_spend,
           AVG(age) as avg_age,
           AVG(loyalty_points) as avg_points
    FROM 's3://airbyte-poc-bucket-cb/relay/synthetic/10m-customers/*.parquet'
    GROUP BY is_active
""").to_df()

print(result.to_string())
print()

# Query 3: High-value customers
print("\nQuery 3: Top 20 High-Value Customers")
print("-" * 60)

result = duckdb.query("""
    SELECT first_name, last_name, email, country, 
           total_spend, loyalty_points
    FROM 's3://airbyte-poc-bucket-cb/relay/synthetic/10m-customers/*.parquet'
    WHERE is_active = true
    ORDER BY total_spend DESC
    LIMIT 20
""").to_df()

print(result.to_string())
print()

# Query 4: Age demographics
print("\nQuery 4: Customer Age Demographics")
print("-" * 60)

result = duckdb.query("""
    SELECT 
        CASE 
            WHEN age < 25 THEN '18-24'
            WHEN age < 35 THEN '25-34'
            WHEN age < 45 THEN '35-44'
            WHEN age < 55 THEN '45-54'
            WHEN age < 65 THEN '55-64'
            ELSE '65+'
        END as age_group,
        COUNT(*) as customers,
        AVG(total_spend) as avg_spend,
        SUM(total_spend) as total_revenue
    FROM 's3://airbyte-poc-bucket-cb/relay/synthetic/10m-customers/*.parquet'
    GROUP BY age_group
    ORDER BY 
        CASE age_group
            WHEN '18-24' THEN 1
            WHEN '25-34' THEN 2
            WHEN '35-44' THEN 3
            WHEN '45-54' THEN 4
            WHEN '55-64' THEN 5
            ELSE 6
        END
""").to_df()

print(result.to_string())
print()

print("=" * 60)
print("DEMO COMPLETE")
print()
print("Key Points:")
print("- Queried 10M rows directly from S3")
print("- No data loading required")
print("- Results in seconds")
print("- Cost: $0 (local processing)")
print("=" * 60)
