"""Verify the agent's Q4 2024 answer"""
import requests

BASE = 'http://localhost:8001/api/v1'

# Get pipelines
r = requests.get(f'{BASE}/pipeline/list')
pipes = r.json()['pipelines']

# Find the e-commerce pipelines
customers_pipe = next((p['id'] for p in pipes if 'customer' in p['name'].lower() and 'final' in p['name'].lower()), None)
orders_pipe = next((p['id'] for p in pipes if 'order' in p['name'].lower() and 'final' in p['name'].lower() and 'item' not in p['name'].lower()), None)
items_pipe = next((p['id'] for p in pipes if 'item' in p['name'].lower() and 'final' in p['name'].lower()), None)

print(f"Using pipelines:")
print(f"  Customers: {customers_pipe}")
print(f"  Orders: {orders_pipe}")
print(f"  Items: {items_pipe}")

if all([customers_pipe, orders_pipe, items_pipe]):
    # Query Q4 2024 revenue by state
    sql = """
    SELECT c.state, 
           COUNT(DISTINCT o.order_id) as total_orders,
           SUM(oi.quantity * oi.price) as total_revenue
    FROM final_test_customers c
    JOIN final_test_orders o ON c.customer_id = o.customer_id
    JOIN final_test_items oi ON o.order_id = oi.order_id
    WHERE o.order_date >= '2024-10-01' AND o.order_date <= '2024-12-31'
    GROUP BY c.state
    ORDER BY total_revenue DESC
    LIMIT 5
    """
    
    r = requests.post(f'{BASE}/query', json={
        'pipelines': [customers_pipe, orders_pipe, items_pipe],
        'sql': sql
    })
    
    if r.status_code == 200:
        data = r.json()
        print("\n" + "="*70)
        print("Q4 2024 REVENUE BY STATE (Top 5)")
        print("="*70)
        
        for i, row in enumerate(data['rows'], 1):
            print(f"{i}. {row['state']:5} - ${row['total_revenue']:>13,.2f}  ({row['total_orders']:>5,} orders)")
        
        # Check if MI is #1
        winner = data['rows'][0]
        print("\n" + "="*70)
        print(f"WINNER: {winner['state']}")
        print(f"Revenue: ${winner['total_revenue']:,.2f}")
        print(f"Orders: {winner['total_orders']:,}")
        print("="*70)
        
        # Compare to agent's answer
        print("\nAGENT'S ANSWER:")
        print("  State: Michigan (MI)")
        print("  Revenue: $10,753,465.40")
        print("  Orders: 4,446")
        
        if winner['state'] == 'MI':
            print("\n✅ CORRECT STATE!")
            rev_match = abs(winner['total_revenue'] - 10753465.40) < 1
            if rev_match:
                print("✅ REVENUE MATCHES!")
            else:
                print(f"⚠️  Revenue difference: ${abs(winner['total_revenue'] - 10753465.40):,.2f}")
        else:
            print(f"\n❌ WRONG! Should be {winner['state']}, not MI")
    else:
        print(f"Error: {r.status_code}")
        print(r.text[:500])
else:
    print("ERROR: Could not find all e-commerce pipelines")
