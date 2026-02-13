# Ready for Fresh Agent Test V2

## What Was Fixed

### 1. **S3 Authentication** ✅
**Problem:** Agent couldn't create pipelines due to missing AWS credentials

**Solution:**
- Modified `src/pipeline.py` to explicitly read AWS env vars
- Relay now running with credentials pre-loaded
- Test pipeline successfully wrote 50K rows to S3 in 1.07 seconds

**Verification:**
```bash
python test_s3_pipeline_simple.py
# Result: SUCCESS - 50,000 rows uploaded to S3
```

### 2. **Clarified Business Question** ✅
**Problem:** Ambiguous - "spent" could mean all orders or completed only

**Before:**
> "Which customers in California spent over $10,000 in 2024..."

**After:**
> "Which customers in California spent over $10,000 in 2024 on COMPLETED orders..."

**Impact:**
- Ground truth: 228 customers (completed only)
- Agent's previous answer: 415 customers (all statuses)
- New question eliminates ambiguity

### 3. **Updated Test Data** ✅
- Data regenerated with same seed (results unchanged)
- `CORRECT_ANSWER.txt` updated with clarified question
- Top customer: Sarah Moore, $30,920.72

---

## Current Status

### Services Running:
- ✅ **Relay API:** http://localhost:8001 (with AWS credentials)
- ✅ **HTTP Server:** http://localhost:8002 (serving CSVs)
- ✅ **S3 Access:** Verified working (bucket: airbyte-poc-bucket-cb)

### Test Data:
- ✅ `customers.csv` - 50,000 rows (3.6 MB)
- ✅ `orders.csv` - 141,303 rows (4.6 MB)
- ✅ `order_items.csv` - 634,948 rows (21 MB)
- ✅ **Total:** 826,251 rows

### Ground Truth Answer:
```
Top 10 CA customers (>$10K in 2024, completed orders):

1. Sarah Moore          $30,920.72  (8 orders, $3,865 avg)
2. John Jones           $29,598.72  (5 orders, $5,920 avg)
3. Mary Davis           $29,292.59  (7 orders, $4,185 avg)
4. Joseph Martin        $27,878.84  (7 orders, $3,983 avg)
5. William Johnson      $24,410.74  (5 orders, $4,882 avg)
6. Richard Jackson      $23,389.02  (4 orders, $5,847 avg)
7. David Miller         $23,197.96  (2 orders, $11,599 avg)
8. Elizabeth Gonzalez   $22,665.73  (7 orders, $3,238 avg)
9. Karen Davis          $22,448.08  (4 orders, $5,612 avg)
10. Joseph Jones        $21,513.61  (6 orders, $3,586 avg)

Total customers: 228
```

---

## Fresh Agent Prompt

**File:** `FRESH_AGENT_FINAL.txt`

**Copy this entire prompt into a fresh agent session:**

```
# E-commerce Analysis - Fresh Agent Test (FINAL)

## Context

You have access to **Relay** (http://localhost:8001), an agent-native data platform for autonomous data movement and transformation.

**Relay is fully configured with AWS S3 credentials** - pipelines will automatically upload data to S3.

## Available Data

Three CSV files containing realistic e-commerce data:

- **Customers:** http://localhost:8002/customers.csv (50,000 rows)
- **Orders:** http://localhost:8002/orders.csv (141,303 rows)  
- **Order Items:** http://localhost:8002/order_items.csv (634,948 rows)

**Total:** ~826,000 rows

## Business Question

**Which customers in California spent over $10,000 in 2024 on COMPLETED orders, and what was their average order value? Show the top 10 by total spend.**

### Critical Requirements:
- **State:** California (CA) only
- **Year:** 2024 only  
- **Status:** COMPLETED orders only (orders table has status column - exclude 'pending', 'shipped', 'cancelled')
- **Threshold:** Total spend > $10,000
- **Calculation:** Line total = order_items.quantity × order_items.price
- **Aggregation:** Sum line totals by customer, count distinct orders
- **Output:** Top 10 customers sorted by total_spend DESC

## Your Task

1. **Load all three datasets into Relay**
   - Create 3 pipelines (one for each CSV)
   - Relay will automatically upload to S3
   - Wait for each pipeline to complete
   
2. **Discover relationships**
   - Use Relay's metadata/search APIs to understand the schema
   - Identify join keys:
     - customers.customer_id = orders.customer_id
     - orders.order_id = order_items.order_id
   
3. **Transform the data**
   - Join all 3 tables
   - Filter: state = 'CA' AND order_date LIKE '2024%' AND status = 'completed'
   - Calculate: line_total = quantity * price
   - Group by customer_id, name
   - Aggregate: SUM(line_total) as total_spend, COUNT(DISTINCT order_id) as num_orders
   - Calculate: avg_order_value = total_spend / num_orders
   - Filter: total_spend > 10000
   - Sort: total_spend DESC
   - Limit: 10
   
4. **Present the answer**
   - Show: customer name, total spend, average order value, number of orders
   - Format clearly (table or list)

## Expected Answer (for validation)

The top customer should be:
- **Sarah Moore** from San Francisco
- **Total spend:** ~$30,920
- **Avg order value:** ~$3,865
- **Orders:** 8

If you get this customer at #1 with approximately these numbers, you're correct!

## Success Criteria

✓ All 3 datasets loaded into Relay successfully
✓ Data written to S3 (check pipeline status)
✓ Correct join keys identified
✓ Filters applied correctly (CA + 2024 + completed)
✓ Math correct (quantity × price, then summed)
✓ Top 10 list sorted by spend
✓ Sarah Moore is #1

## Technical Notes

- Relay API docs: http://localhost:8001/api/v1/capabilities
- S3 bucket: airbyte-poc-bucket-cb (already configured)
- Expected pipeline create format:
  ```json
  {
    "name": "Pipeline Name",
    "source": {"type": "csv_url", "url": "http://localhost:8002/file.csv"},
    "destination": {"type": "s3", "bucket": "airbyte-poc-bucket-cb", "path": "relay/your-path"}
  }
  ```

## Test Goal

Can you complete this complex multi-table analysis end-to-end without any help? This validates whether Relay is truly agent-native.

Good luck! 🚀
```

---

## Validation After Test

Compare agent's answer to `CORRECT_ANSWER.txt`:

```bash
cat CORRECT_ANSWER.txt
```

**Success =** Sarah Moore at #1 with ~$30,920 total spend

---

## What This Tests

### End-to-End Workflow:
1. ✅ Pipeline creation (3x)
2. ✅ S3 data upload (826K rows)
3. ✅ Schema discovery
4. ✅ Relationship inference (2 joins)
5. ✅ Complex filtering (3 conditions)
6. ✅ Mathematical calculations
7. ✅ Aggregation and sorting

### Agent Capabilities:
- API discovery (`/capabilities`)
- Data loading (CSV → S3)
- Metadata analysis
- Join key identification
- SQL-like transformations
- Result validation

### Platform Validation:
- S3 auth works seamlessly
- Pipelines handle large datasets
- Metadata generation works
- No manual intervention needed

---

## If Agent Succeeds

**This proves:**
- Relay is truly agent-native
- S3 auth friction is resolved
- Complex multi-table queries work
- 826K rows is manageable
- Agent can operate independently

---

## If Agent Fails

**Debug checklist:**
1. Did pipelines complete? (Check `/pipeline/list`)
2. Was data written to S3? (Check pipeline status)
3. Did agent find correct join keys?
4. Was status='completed' filter applied?
5. Is math correct? (quantity × price)

---

**Ready to test! Copy prompt from `FRESH_AGENT_FINAL.txt` and paste into fresh agent.**
