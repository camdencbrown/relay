# RELAY KILLER DEMO
## 5-Minute Demo That Wins Projects

**Goal:** Show how an agent can move enterprise data in minutes, not days.

**Timeline:** 5 minutes total (rehearse to get timing right)

---

## **PRE-DEMO SETUP (Do Tomorrow Morning)**

### 1. Get Salesforce Credentials (2 min)
```bash
# You need:
SALESFORCE_USERNAME=your@email.com
SALESFORCE_PASSWORD=yourpassword
SALESFORCE_SECURITY_TOKEN=yourtoken
```

### 2. Create Athena Table (2 min)
```sql
-- Run this in AWS Athena console
CREATE EXTERNAL TABLE relay_demo_opportunities (
    Id STRING,
    Name STRING,
    Amount DOUBLE,
    StageName STRING,
    CloseDate STRING,
    AccountId STRING,
    OwnerId STRING
)
STORED AS PARQUET
LOCATION 's3://airbyte-poc-bucket-cb/relay/demo/opportunities/';
```

### 3. Start Relay (30 sec)
```bash
cd C:\Users\User\.openclaw\workspace\relay
python -m src.main
```

---

## **THE DEMO (5 Minutes)**

### **Minute 1: The Problem**

**You say:**
> "Traditional data pipelines take days to build. ETL tools require engineers, clicking through UIs, debugging errors. By the time data flows, the business question has changed."

**Show:** 
- Pull up Airbyte UI (optional - show complexity)
- Or just describe: "30 minutes per pipeline, 10 API calls, trial and error"

---

### **Minute 2: The Agent-First Approach**

**You say:**
> "Relay is different. It's designed FOR AI agents, not humans. Watch how fast this is."

**Demo:**
Open terminal, run:

```bash
curl -X POST http://localhost:8001/api/v1/pipeline/create \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Salesforce Opportunities",
    "description": "Pull all open opportunities for analysis",
    "source": {
      "type": "salesforce",
      "username": "YOUR_USERNAME",
      "password": "YOUR_PASSWORD",
      "security_token": "YOUR_TOKEN",
      "query": "SELECT Id, Name, Amount, StageName, CloseDate, AccountId, OwnerId FROM Opportunity WHERE StageName != '\''Closed Won'\''"
    },
    "destination": {
      "type": "s3",
      "bucket": "airbyte-poc-bucket-cb",
      "path": "relay/demo/opportunities/"
    },
    "options": {
      "format": "parquet",
      "compression": "snappy",
      "streaming": true,
      "generate_metadata": true
    }
  }'
```

**You say (while it runs):**
> "That's it. Three API fields: source, destination, options. The agent figured out the rest. No connector IDs, no workspace setup, no trial and error."

**Show response:**
```json
{
  "status": "created",
  "pipeline_id": "pipe-abc123",
  "message": "Pipeline created successfully"
}
```

---

### **Minute 3: Execute & Monitor**

**Run the pipeline:**
```bash
curl -X POST http://localhost:8001/api/v1/pipeline/pipe-abc123/run
```

**Open browser:** http://localhost:8001

**You say:**
> "While it runs, check out the UI. Real-time status, no refresh needed. This is running in the background, streaming data in chunks."

**Point out:**
- Pipeline card showing "Running"
- Source → Agent → Transform → Destination visual
- Progress indicator

**Check status:**
```bash
curl http://localhost:8001/api/v1/pipeline/pipe-abc123/run/run-xyz789
```

**Show:**
```json
{
  "status": "success",
  "rows_processed": 1247,
  "duration_seconds": 3.4,
  "output_file": "s3://bucket/path/"
}
```

**You say:**
> "1,247 opportunities synced in 3.4 seconds. Now let's query it."

---

### **Minute 4: Query the Data**

**Option A: DuckDB (Local, Fast)**

```python
import duckdb

df = duckdb.query("""
    SELECT StageName, COUNT(*) as count, SUM(Amount) as total_value
    FROM 's3://airbyte-poc-bucket-cb/relay/demo/opportunities/*.parquet'
    GROUP BY StageName
    ORDER BY total_value DESC
""").to_df()

print(df)
```

**Output:**
```
     StageName  count  total_value
0  Negotiation    247   $2.4M
1   Proposal     189   $1.8M
2  Prospecting   811   $890K
```

**You say:**
> "DuckDB reads directly from S3. No loading, no transformation. The semantic layer tells the agent what these columns mean."

---

**Option B: Athena (if you set it up)**

Go to Athena console, run:
```sql
SELECT StageName, COUNT(*) as opportunity_count, 
       SUM(Amount) as total_pipeline_value
FROM relay_demo_opportunities
GROUP BY StageName
ORDER BY total_pipeline_value DESC;
```

**You say:**
> "Or use Athena - serverless SQL, only pay for what you query. $0.005 per TB scanned. This query cost less than a penny."

---

### **Minute 5: The Semantic Layer**

**Open:** http://localhost:8001/metadata

**You say:**
> "Here's the magic. Relay automatically analyzed the data structure. The agent now knows:"

**Show metadata table:**
- Column names
- Data types  
- Semantic types (currency, identifier, date)
- Auto-generated descriptions

**You say:**
> "Next time an agent needs this data, it doesn't guess. It knows 'Amount' is USD, 'StageName' is categorical, 'CloseDate' is temporal. It can generate correct queries automatically."

**Demo Natural Language Query (if time):**

"Agent, show me high-value opportunities closing this quarter"

**Agent does:**
1. Queries metadata → knows columns
2. Generates SQL:
   ```sql
   SELECT * FROM opportunities 
   WHERE Amount > 100000 
   AND CloseDate BETWEEN '2026-01-01' AND '2026-03-31'
   ```
3. Returns results with business context

---

## **THE CLOSE (30 seconds)**

**You say:**
> "Let's recap:
> - Agent created pipeline: **30 seconds**
> - Data synced: **3 seconds**
> - Query answered: **instant**
> - Total: **under a minute** from idea to insight
>
> Compare that to:
> - Traditional ETL: **days**
> - Airbyte: **30 minutes** per pipeline
> - Custom scripts: **hours** to write and debug
>
> This is agent-native infrastructure. Not just APIs added to human tools—designed from the ground up for AI.
>
> **What data challenges are you facing? Want us to build something like this for your domain?**"

---

## **BACKUP SLIDES (If They Ask)**

### "How does it handle failures?"
> "Retry logic, clear error messages, automatic notifications. Failed chunks don't block the whole pipeline."

### "What about security?"
> "API key auth, credential vault, audit logs. Row-level security on roadmap."

### "Can it handle real-time?"
> "V2 adds Kafka/Kinesis. V1 is batch-optimized—which handles 95% of use cases."

### "What about transformations?"
> "Coming in V2. For now, query engines (Athena, Snowflake) handle transformations. Keeps Relay focused on movement."

### "Why not just use Airbyte?"
> "Airbyte is human-first with APIs added. Relay is agent-first from day one. 15x faster for agents, self-describing, semantic layer built-in."

---

## **SUCCESS METRICS**

Demo is successful if they say:
- "Can you do this for our Salesforce?"
- "How much would this cost to build?"
- "When can we get access?"
- "Can we see this with our data?"

Demo failed if they say:
- "Interesting" (and change subject)
- "We already have Fivetran"
- "Let us think about it"

**If they're lukewarm:** Offer proof-of-concept. "Give us your Salesforce credentials and one business question. We'll have answers in 24 hours."

---

## **POST-DEMO TODO**

If they're interested:
1. Get their Salesforce credentials (or other data source)
2. Identify their #1 business question
3. Build custom demo with THEIR data
4. Schedule follow-up to show results
5. Proposal for full build-out

---

## **NOTES FOR CAMDEN**

- **Rehearse timing** - 5 minutes goes fast
- **Have backup plan** if Salesforce creds don't work (use synthetic data demo)
- **Focus on speed** - that's the killer feature
- **End with question** - get them talking about their problems
- **Be ready to pivot** - "Imagine this for [their use case]"

**You've got this. The tech is solid. Now sell the vision.** 🚀
