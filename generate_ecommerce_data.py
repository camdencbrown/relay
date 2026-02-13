"""
Generate realistic e-commerce test data for Relay stress test.

Creates 3 datasets:
- 50K customers
- 200K orders
- 500K order line items

Ensures we know the answer to the test question in advance.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

print("Generating e-commerce datasets...")

# ============================================================================
# CUSTOMERS (50,000 rows)
# ============================================================================
print("\n1. Generating customers...")

states = ['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
state_weights = [0.12, 0.10, 0.09, 0.08, 0.07, 0.06, 0.06, 0.05, 0.05, 0.32]  # CA is 12%

cities_by_state = {
    'CA': ['Los Angeles', 'San Francisco', 'San Diego', 'San Jose', 'Sacramento'],
    'NY': ['New York', 'Buffalo', 'Rochester', 'Albany', 'Syracuse'],
    'TX': ['Houston', 'Dallas', 'Austin', 'San Antonio', 'Fort Worth'],
    'FL': ['Miami', 'Tampa', 'Orlando', 'Jacksonville', 'Fort Lauderdale'],
    'IL': ['Chicago', 'Aurora', 'Naperville', 'Joliet', 'Rockford'],
}

first_names = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 
               'William', 'Barbara', 'David', 'Elizabeth', 'Richard', 'Susan', 'Joseph', 'Jessica',
               'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Nancy', 'Daniel', 'Lisa']

last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
              'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
              'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Thompson', 'White', 'Harris']

num_customers = 50000
customer_ids = range(1, num_customers + 1)

customers_data = []
for cid in customer_ids:
    state = np.random.choice(states, p=state_weights)
    city = random.choice(cities_by_state.get(state, ['Unknown']))
    first = random.choice(first_names)
    last = random.choice(last_names)
    name = f"{first} {last}"
    email = f"{first.lower()}.{last.lower()}{cid}@example.com"
    
    # Random signup date between 2020-2023
    days_ago = random.randint(365, 365*4)
    signup_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    
    customers_data.append({
        'customer_id': cid,
        'name': name,
        'email': email,
        'state': state,
        'city': city,
        'signup_date': signup_date
    })

customers_df = pd.DataFrame(customers_data)
print(f"   Created {len(customers_df)} customers")
print(f"   California customers: {len(customers_df[customers_df.state == 'CA'])}")

# ============================================================================
# ORDERS (200,000 rows)
# ============================================================================
print("\n2. Generating orders...")

# Orders distributed across customers (some have many, some have few, some have none)
# Average ~4 orders per customer, but heavily skewed
orders_data = []
order_id = 1

for cid in customer_ids:
    # 20% of customers have no orders
    if random.random() < 0.20:
        continue
    
    # Number of orders: heavily skewed (most have 1-3, some have 20+)
    num_orders = int(np.random.exponential(3)) + 1
    num_orders = min(num_orders, 25)  # Cap at 25
    
    for _ in range(num_orders):
        # Order date: 2022-2025, with more recent bias
        year = random.choices([2022, 2023, 2024, 2025], weights=[0.1, 0.2, 0.4, 0.3])[0]
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        order_date = f"{year}-{month:02d}-{day:02d}"
        
        status = random.choices(['completed', 'shipped', 'pending', 'cancelled'], 
                                weights=[0.7, 0.2, 0.05, 0.05])[0]
        
        orders_data.append({
            'order_id': order_id,
            'customer_id': cid,
            'order_date': order_date,
            'status': status
        })
        order_id += 1

orders_df = pd.DataFrame(orders_data)
print(f"   Created {len(orders_df)} orders")
print(f"   Orders in 2024: {len(orders_df[orders_df.order_date.str.startswith('2024')])}")

# ============================================================================
# ORDER ITEMS (500,000 rows)
# ============================================================================
print("\n3. Generating order items...")

products = [
    ('Laptop Pro', 1200, 1800),
    ('Wireless Mouse', 25, 50),
    ('Keyboard', 80, 150),
    ('Monitor 27"', 300, 600),
    ('USB-C Cable', 15, 30),
    ('Webcam HD', 60, 120),
    ('Desk Chair', 200, 400),
    ('Standing Desk', 400, 800),
    ('Headphones', 100, 300),
    ('Phone Case', 20, 40),
    ('Tablet', 400, 800),
    ('Smartwatch', 250, 500),
    ('Charger', 30, 60),
    ('External SSD', 100, 200),
    ('HDMI Cable', 10, 25),
]

items_data = []
item_id = 1

for _, order_row in orders_df.iterrows():
    # 3-5 items per order on average
    num_items = random.randint(1, 8)
    
    for _ in range(num_items):
        product_name, min_price, max_price = random.choice(products)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.1, 0.05, 0.05])[0]
        price = round(random.uniform(min_price, max_price), 2)
        
        items_data.append({
            'item_id': item_id,
            'order_id': order_row['order_id'],
            'product_name': product_name,
            'quantity': quantity,
            'price': price
        })
        item_id += 1

items_df = pd.DataFrame(items_data)
print(f"   Created {len(items_df)} order items")

# ============================================================================
# CALCULATE THE ANSWER IN ADVANCE
# ============================================================================
print("\n" + "="*70)
print("CALCULATING THE CORRECT ANSWER...")
print("="*70)

# Join all three tables
full_data = items_df.merge(orders_df, on='order_id')
full_data = full_data.merge(customers_df, on='customer_id')

# Filter: California, 2024, completed orders
ca_2024 = full_data[
    (full_data['state'] == 'CA') & 
    (full_data['order_date'].str.startswith('2024')) &
    (full_data['status'] == 'completed')
].copy()

print(f"\nFiltered to CA + 2024 + completed: {len(ca_2024)} line items")

# Calculate line total
ca_2024['line_total'] = ca_2024['quantity'] * ca_2024['price']

# Group by customer
customer_summary = ca_2024.groupby('customer_id').agg({
    'line_total': 'sum',
    'order_id': 'nunique',
    'name': 'first',
    'email': 'first',
    'city': 'first'
}).reset_index()

customer_summary.columns = ['customer_id', 'total_spend', 'num_orders', 'name', 'email', 'city']

# Calculate average order value
customer_summary['avg_order_value'] = customer_summary['total_spend'] / customer_summary['num_orders']

# Filter: spent over $10,000
high_spenders = customer_summary[customer_summary['total_spend'] > 10000].copy()

# Sort and get top 10
top_10 = high_spenders.sort_values('total_spend', ascending=False).head(10)

print("\n" + "="*70)
print("ANSWER TO BUSINESS QUESTION:")
print("="*70)
print("\nQuestion: Which customers in California spent over $10,000 in 2024")
print("          on COMPLETED orders, and what was their average order value?")
print("          Show top 10 by total spend.\n")

for idx, row in top_10.iterrows():
    print(f"{row['name']:25s} | Total: ${row['total_spend']:>10,.2f} | Avg Order: ${row['avg_order_value']:>8,.2f} | Orders: {row['num_orders']:>2.0f} | City: {row['city']}")

print("\n" + "="*70)
print(f"Total CA customers who spent >$10K in 2024: {len(high_spenders)}")
print("="*70)

# ============================================================================
# SAVE TO CSV
# ============================================================================
print("\nSaving datasets...")

customers_df.to_csv('customers.csv', index=False)
orders_df.to_csv('orders.csv', index=False)
items_df.to_csv('order_items.csv', index=False)

print(f"\nCreated:")
print(f"   - customers.csv ({len(customers_df):,} rows)")
print(f"   - orders.csv ({len(orders_df):,} rows)")
print(f"   - order_items.csv ({len(items_df):,} rows)")
print(f"   - TOTAL: {len(customers_df) + len(orders_df) + len(items_df):,} rows")

# Save the answer
with open('CORRECT_ANSWER.txt', 'w') as f:
    f.write("="*70 + "\n")
    f.write("CORRECT ANSWER TO BUSINESS QUESTION\n")
    f.write("="*70 + "\n\n")
    f.write("Question: Which customers in California spent over $10,000 in 2024\n")
    f.write("          on COMPLETED orders, and what was their average order value?\n")
    f.write("          Show top 10 by total spend.\n\n")
    f.write("-"*70 + "\n")
    f.write(f"{'Name':<25} | {'Total Spend':>12} | {'Avg Order':>10} | {'Orders':>7} | City\n")
    f.write("-"*70 + "\n")
    
    for idx, row in top_10.iterrows():
        f.write(f"{row['name']:25s} | ${row['total_spend']:>11,.2f} | ${row['avg_order_value']:>9,.2f} | {row['num_orders']:>7.0f} | {row['city']}\n")
    
    f.write("-"*70 + "\n")
    f.write(f"\nTotal CA customers who spent >$10K in 2024: {len(high_spenders)}\n")

print(f"   - CORRECT_ANSWER.txt (reference for validation)")
print("\nDone! Ready for fresh agent test.")
