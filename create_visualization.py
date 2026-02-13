import json
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

# Read the data
with open('profit_by_category.json', 'r') as f:
    data = json.load(f)

# Extract data for visualization
categories = [item['category'] for item in data]
profits = [item['profit'] for item in data]
profit_margins = [item['profit_margin_pct'] for item in data]

# Create figure with two subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Bar chart for profit by category
colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E']
bars = ax1.bar(categories, profits, color=colors, edgecolor='black', linewidth=1.5)
ax1.set_xlabel('Category', fontsize=12, fontweight='bold')
ax1.set_ylabel('Profit ($)', fontsize=12, fontweight='bold')
ax1.set_title('2024 Profit by Product Category', fontsize=14, fontweight='bold', pad=20)
ax1.grid(axis='y', alpha=0.3, linestyle='--')
ax1.set_axisbelow(True)

# Rotate x-axis labels
ax1.tick_params(axis='x', rotation=45)

# Add value labels on bars
for bar in bars:
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
            f'${height:,.0f}',
            ha='center', va='bottom', fontsize=10, fontweight='bold')

# Bar chart for profit margin
bars2 = ax2.bar(categories, profit_margins, color=colors, edgecolor='black', linewidth=1.5)
ax2.set_xlabel('Category', fontsize=12, fontweight='bold')
ax2.set_ylabel('Profit Margin (%)', fontsize=12, fontweight='bold')
ax2.set_title('2024 Profit Margin by Product Category', fontsize=14, fontweight='bold', pad=20)
ax2.grid(axis='y', alpha=0.3, linestyle='--')
ax2.set_axisbelow(True)

# Rotate x-axis labels
ax2.tick_params(axis='x', rotation=45)

# Add value labels on bars
for bar in bars2:
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}%',
            ha='center', va='bottom', fontsize=10, fontweight='bold')

# Adjust layout and save
plt.tight_layout()
plt.savefig('profit_visualization.png', dpi=300, bbox_inches='tight')
print("Visualization saved as profit_visualization.png")

# Also create a summary report
print("\n=== 2024 PRODUCT PERFORMANCE ANALYSIS ===\n")
print("Profit by Category (Ranked):")
print("-" * 50)
for i, item in enumerate(data, 1):
    print(f"{i}. {item['category']:15s} | Profit: ${item['profit']:>12,.2f} | Margin: {item['profit_margin_pct']:>5.1f}%")
print("-" * 50)
print(f"\nTop Performer: {data[0]['category']}")
print(f"Highest Profit: ${data[0]['profit']:,.2f}")
print(f"Best Profit Margin: {data[0]['profit_margin_pct']:.1f}%")
