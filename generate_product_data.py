"""
Generate product performance data for end-to-end test
Complex multi-table scenario with hidden answer
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(99)
random.seed(99)

print("Generating product performance datasets...")

# Products (50 products across 5 categories)
categories = ["Electronics", "Home & Garden", "Sports", "Fashion", "Books"]
products_data = []

for i in range(1, 51):
    cat = categories[(i-1) // 10]
    products_data.append({
        "product_id": i,
        "product_name": f"Product {i}",
        "category": cat,
        "price": round(random.uniform(10, 500), 2),
        "cost": round(random.uniform(5, 300), 2)
    })

products_df = pd.DataFrame(products_data)

# Calculate profit margin
products_df["profit_margin"] = ((products_df["price"] - products_df["cost"]) / products_df["price"] * 100).round(2)

# Sales (10,000 transactions over 2024)
sales_data = []
start_date = datetime(2024, 1, 1)

for i in range(1, 10001):
    # Electronics sell more in Nov/Dec (holiday season)
    # Sports sell more in summer
    # Books sell consistently
    
    days_offset = random.randint(0, 365)
    sale_date = start_date + timedelta(days=days_offset)
    month = sale_date.month
    
    # Bias product selection based on seasonality
    if month in [11, 12]:  # Holiday season
        product_id = random.choices(range(1, 51), weights=[3 if i <= 10 else 1 for i in range(1, 51)])[0]
    elif month in [6, 7, 8]:  # Summer
        product_id = random.choices(range(1, 51), weights=[3 if 21 <= i <= 30 else 1 for i in range(1, 51)])[0]
    else:
        product_id = random.randint(1, 50)
    
    quantity = random.choices([1, 2, 3, 4, 5], weights=[0.5, 0.25, 0.15, 0.07, 0.03])[0]
    
    sales_data.append({
        "sale_id": i,
        "product_id": product_id,
        "sale_date": sale_date.strftime("%Y-%m-%d"),
        "quantity": quantity,
        "region": random.choice(["North", "South", "East", "West"])
    })

sales_df = pd.DataFrame(sales_data)

# Customer reviews (3,000 reviews)
reviews_data = []
for i in range(1, 3001):
    product_id = random.randint(1, 50)
    
    # Products 1-5 have higher ratings (hidden gem)
    if product_id <= 5:
        rating = random.choices([3, 4, 5], weights=[0.1, 0.3, 0.6])[0]
    else:
        rating = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.1, 0.25, 0.35, 0.25])[0]
    
    reviews_data.append({
        "review_id": i,
        "product_id": product_id,
        "rating": rating,
        "review_date": (start_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
    })

reviews_df = pd.DataFrame(reviews_data)

# Save files
products_df.to_csv("products.csv", index=False)
sales_df.to_csv("sales.csv", index=False)
reviews_df.to_csv("reviews.csv", index=False)

print("\nDatasets created:")
print(f"  - products.csv: {len(products_df)} products")
print(f"  - sales.csv: {len(sales_df)} sales")
print(f"  - reviews.csv: {len(reviews_df)} reviews")

# Calculate the hidden answer
print("\n" + "="*70)
print("GROUND TRUTH ANSWER (Hidden from agent)")
print("="*70)

# Join everything
full = sales_df.merge(products_df, on="product_id")
full["revenue"] = full["quantity"] * full["price"]
full["profit"] = full["quantity"] * (full["price"] - full["cost"])

# Get average rating per product
avg_ratings = reviews_df.groupby("product_id")["rating"].mean().reset_index()
avg_ratings.columns = ["product_id", "avg_rating"]

# Join with ratings
full = full.merge(avg_ratings, on="product_id", how="left")

# Group by category
category_stats = full.groupby("category").agg({
    "revenue": "sum",
    "profit": "sum",
    "quantity": "sum",
    "avg_rating": "mean"
}).reset_index()

category_stats["profit_margin_pct"] = (category_stats["profit"] / category_stats["revenue"] * 100).round(2)
category_stats = category_stats.sort_values("profit", ascending=False)

print("\nQuestion: Which product category generated the most profit in 2024?")
print("\nAnswer by category (sorted by profit):")
print(category_stats.to_string(index=False))

print("\n" + "="*70)
print(f"TOP PERFORMER: {category_stats.iloc[0]['category']}")
print(f"Profit: ${category_stats.iloc[0]['profit']:,.2f}")
print(f"Revenue: ${category_stats.iloc[0]['revenue']:,.2f}")
print(f"Profit Margin: {category_stats.iloc[0]['profit_margin_pct']}%")
print("="*70)

# Save answer
with open("GROUND_TRUTH_ANSWER.txt", "w") as f:
    f.write("GROUND TRUTH ANSWER\n")
    f.write("="*70 + "\n\n")
    f.write("Question: Which product category generated the most profit in 2024?\n\n")
    f.write(f"Answer: {category_stats.iloc[0]['category']}\n")
    f.write(f"Total Profit: ${category_stats.iloc[0]['profit']:,.2f}\n")
    f.write(f"Total Revenue: ${category_stats.iloc[0]['revenue']:,.2f}\n")
    f.write(f"Profit Margin: {category_stats.iloc[0]['profit_margin_pct']}%\n\n")
    f.write("Full Rankings:\n")
    f.write(category_stats.to_string(index=False))

print("\nGround truth saved to GROUND_TRUTH_ANSWER.txt")
print("\nReady for fresh agent test!")
