# Start Relay
# AWS credentials are loaded from .env file automatically

Write-Host "Starting Relay..." -ForegroundColor Green
Write-Host "Credentials loaded from .env file" -ForegroundColor Cyan
Write-Host ""

# Check if .env exists
if (-not (Test-Path ".env")) {
    Write-Host "⚠️  WARNING: .env file not found!" -ForegroundColor Yellow
    Write-Host "Please copy .env.example to .env and configure your credentials" -ForegroundColor Yellow
    Write-Host ""
    exit 1
}

# Start server
python -m uvicorn src.main:app --host 0.0.0.0 --port 8001
