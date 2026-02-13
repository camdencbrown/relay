# 🚨 GitHub Setup - READ BEFORE PUSHING 🚨

## ✅ Security Checklist

Before pushing to GitHub, verify:

1. **✅ `.env` file is gitignored** (contains your AWS credentials)
2. **✅ No credentials in code** (all secrets use environment variables)
3. **✅ `.env.example` is safe to commit** (template only, no real credentials)
4. **✅ `awssecret.txt` never committed** (your desktop file stays local)

---

## 📋 Setup for Team Members

### 1. Clone the Repository

```bash
git clone https://github.com/your-org/relay.git
cd relay
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure AWS Credentials

**Option A: Environment File (Recommended)**

1. Copy the example file:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your AWS credentials:
   ```bash
   AWS_ACCESS_KEY_ID=your_access_key_here
   AWS_SECRET_ACCESS_KEY=your_secret_key_here
   AWS_DEFAULT_REGION=us-west-1
   S3_BUCKET_NAME=your-bucket-name
   ```

3. **NEVER commit `.env` to git!** (It's already in `.gitignore`)

**Option B: AWS CLI Configuration**

```bash
aws configure
# Enter your credentials when prompted
```

Relay will automatically use AWS CLI credentials if `.env` is not present.

### 4. Verify AWS Access

```bash
aws s3 ls s3://your-bucket-name/
```

### 5. Start Relay

```bash
python -m uvicorn src.main:app --host 0.0.0.0 --port 8001
```

Visit: http://localhost:8001

---

## 🔐 Security Best Practices

### What's Safe to Commit:
- ✅ Source code (`src/**/*.py`)
- ✅ Requirements (`requirements.txt`)
- ✅ Documentation (`.md` files)
- ✅ Sample data (`products.csv`, `sales.csv`, `reviews.csv`)
- ✅ `.env.example` (template without real credentials)

### What's NEVER Committed:
- ❌ `.env` file (real credentials)
- ❌ `awssecret.txt` or similar credential files
- ❌ `pipelines.json` (may contain sensitive bucket paths)
- ❌ `.aws/` directory
- ❌ `*.log` files (may contain sensitive info)
- ❌ `metadata/*.json` (pipeline metadata)

**Our `.gitignore` already protects you from these!**

---

## 🧪 Testing Without AWS

For local testing without S3, use the `/test_source` endpoint:

```bash
curl -X POST http://localhost:8001/api/v1/pipeline/test \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "csv_url",
    "url": "http://localhost:8003/products.csv"
  }'
```

---

## 🤝 Sharing Access

### Option 1: Shared S3 Bucket
- Create IAM users for each team member
- Give them access keys (send via secure channel, NOT GitHub)
- Each person configures their own `.env` file

### Option 2: Per-User Buckets
- Each developer uses their own S3 bucket
- Configure bucket name in their `.env` file
- No shared credentials needed

### Option 3: IAM Roles (Production)
- Deploy to AWS with IAM role attached
- No credential files needed
- Most secure option

---

## 🛡️ What If Credentials Leak?

If you accidentally commit credentials:

1. **Rotate immediately:**
   ```bash
   aws iam create-access-key --user-name your-user
   aws iam delete-access-key --access-key-id OLD_KEY_ID --user-name your-user
   ```

2. **Remove from Git history:**
   ```bash
   git filter-branch --force --index-filter \
     "git rm --cached --ignore-unmatch .env" \
     --prune-empty --tag-name-filter cat -- --all
   ```

3. **Force push (destructive):**
   ```bash
   git push origin --force --all
   ```

4. **Notify your team** to pull fresh and reconfigure

---

## 📖 Additional Resources

- [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [GitHub Secret Scanning](https://docs.github.com/en/code-security/secret-scanning)
- [python-dotenv Documentation](https://github.com/theskumar/python-dotenv)

---

**Questions?** Check with the team lead before committing if unsure about any file!
