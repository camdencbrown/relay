# Setup AWS credentials for Relay
# DEPRECATED: Use .env file instead!
# This script is kept for reference only.

Write-Host "⚠️  DEPRECATED: This script is no longer used" -ForegroundColor Yellow
Write-Host ""
Write-Host "Please configure credentials in .env file instead:" -ForegroundColor Cyan
Write-Host "  1. Copy .env.example to .env"
Write-Host "  2. Edit .env and add your AWS credentials"
Write-Host "  3. Start Relay with: python -m uvicorn src.main:app --host 0.0.0.0 --port 8001"
Write-Host ""
Write-Host "See GITHUB_SETUP.md for detailed instructions." -ForegroundColor Green
