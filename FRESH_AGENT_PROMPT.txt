# E-commerce Stress Test - Fresh Agent Task

## Context

You have access to **Relay** (http://localhost:8001), an agent-native data platform designed for autonomous data movement and transformation.

## Available Data

Three CSV files containing realistic e-commerce data:

- **Customers:** http://localhost:8002/customers.csv (50,000 rows)
- **Orders:** http://localhost:8002/orders.csv (141,303 rows)  
- **Order Items:** http://localhost:8002/order_items.csv (634,948 rows)

**Total:** ~826,000 rows

## Business Question

**Which customers in California spent over $10,000 in 2024, and what was their average order value? Show the top 10 by total spend.**

## Your Task

1. Load all three datasets into Relay
2. Discover how the datasets relate to each other
3. Combine the data appropriately
4. Answer the business question
5. Verify your answer is correct

## Requirements

- You have **full autonomy** - figure out the approach on your own
- Use Relay's APIs to accomplish this (explore `/api/v1/capabilities` if needed)
- Document your process and reasoning
- Show your final answer in a clear format

## Success Criteria

- ✅ All 3 datasets loaded into Relay
- ✅ Correct join keys identified
- ✅ Proper filtering (California, 2024, >$10K)
- ✅ Top 10 customers listed with total spend and average order value
- ✅ Answer matches the ground truth

## Notes

- Relay is already running at http://localhost:8001
- The CSVs are being served via HTTP at localhost:8002
- You do NOT need help - this tests if Relay is truly agent-native
- Take your time and be thorough

Good luck! 🚀
