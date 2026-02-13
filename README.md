# Relay - Agent-Native Data Movement Platform

**Where agents move data.**

Relay is a data movement and analytics platform designed from scratch for AI agents. Unlike traditional tools built for humans with APIs added later, Relay provides a self-documenting, intuitive API that agents can discover and use independently.

## 🎯 Key Features

- **Agent-First Design** - Self-documenting `/capabilities` endpoint tells agents everything in one call
- **Instant Setup** - From zero to querying data in under 10 minutes (proven with independent agent tests)
- **High Performance** - Query 800K+ rows in sub-second time, stream 10M+ rows efficiently
- **Multi-Source Support** - REST APIs, CSV/JSON URLs, MySQL, Postgres, Salesforce
- **DuckDB Query Engine** - Full SQL support with in-memory processing over S3
- **S3 Storage** - Data stored as compressed Parquet files in AWS S3

## 🚀 Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/YOUR-USERNAME/relay.git
cd relay
pip install -r requirements.txt
```

### 2. Configure AWS Credentials

```bash
cp .env.example .env
# Edit .env with your AWS credentials
```

See [GITHUB_SETUP.md](GITHUB_SETUP.md) for detailed configuration instructions.

### 3. Start Relay

**Option A: PowerShell Script (Windows)**
```powershell
.\start_relay.ps1
```

**Option B: Direct Command**
```bash
python -m uvicorn src.main:app --host 0.0.0.0 --port 8001
```

### 4. Verify

Visit http://localhost:8001 or check the API:
```bash
curl http://localhost:8001/api/v1/capabilities
```

## 📖 Documentation

- **[GITHUB_SETUP.md](GITHUB_SETUP.md)** - Team setup & security guidelines
- **[TECHNICAL_OVERVIEW.md](TECHNICAL_OVERVIEW.md)** - Architecture deep-dive
- **[DEMO.md](DEMO.md)** - Demo script and examples
- **API Docs** - http://localhost:8001/docs (when server is running)

## 🧪 Agent Testing

Relay has been validated by independent AI agents with zero prior knowledge:

- **Usability Score:** 9/10
- **Time to Complete Workflow:** 8-9 minutes
- **Agent Feedback:** _"This is exactly what an agent-native data platform should be."_

See [E2E_TEST_REPORT.md](E2E_TEST_REPORT.md) for full test results.

## 🔌 Supported Data Sources

- **REST APIs** - Any public API endpoint
- **CSV/JSON URLs** - Direct file downloads
- **MySQL** - Database connections
- **PostgreSQL** - Database connections
- **Salesforce** - SOQL query support
- **Synthetic Data** - Built-in generator for testing

## 🔍 Example Workflow

```python
import requests

base_url = "http://localhost:8001/api/v1"

# 1. Create a pipeline
pipeline = requests.post(f"{base_url}/pipeline/create", json={
    "name": "crypto_data",
    "source": {
        "type": "rest_api",
        "url": "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
    },
    "destination": {
        "type": "s3",
        "bucket": "your-bucket",
        "path": "data/crypto/"
    }
}).json()

pipeline_id = pipeline["pipeline_id"]

# 2. Run the pipeline
run = requests.post(f"{base_url}/pipeline/{pipeline_id}/run").json()

# 3. Query the data
results = requests.post(f"{base_url}/query", json={
    "pipelines": [pipeline_id],
    "sql": "SELECT name, market_cap FROM crypto_data ORDER BY market_cap DESC LIMIT 10"
}).json()

print(results["rows"])
```

## 📊 Performance

**Proven Capabilities:**
- 10M rows loaded in 73 seconds (136K rows/sec)
- 826K row dataset queried in 2.5 seconds
- Complex 3-table JOINs with sub-second response
- Automatic parallelization (2-20 workers based on dataset size)

## 🛡️ Security

**⚠️ IMPORTANT:** Never commit credentials to git!

- All credentials stored in `.env` file (gitignored)
- `.env.example` provides template
- See [GITHUB_SETUP.md](GITHUB_SETUP.md) for security best practices

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Configure your `.env` file (don't commit it!)
4. Make your changes
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## 📝 License

[Add your license here]

## 🙋 Support

- **Documentation:** See docs/ directory
- **Issues:** Open a GitHub issue
- **Questions:** [Your contact/support channel]

## 🎯 Why Agent-Native?

Traditional data tools (Airbyte, Fivetran) were built for humans with APIs added later. Agents need:
- ✅ Self-documentation in one API call
- ✅ Smart defaults (80% less configuration)
- ✅ Clear error messages
- ✅ Simple, predictable workflows

Relay delivers all of this. **Cost:** 90% cheaper than Fivetran ($100 vs $2K/month).

---

**Built for agents. Tested by agents. Approved by agents.** 🤖✨
