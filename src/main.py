"""
Relay - Agent-Native Data Movement Platform
Main FastAPI application
"""

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pathlib import Path
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

from .api import router as api_router
from .scheduler import PipelineScheduler
from .storage import Storage
from .pipeline import PipelineEngine

# Initialize components for scheduler
storage = Storage()
engine = PipelineEngine(storage)
scheduler = PipelineScheduler(storage, engine)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: start the scheduler
    await scheduler.start()
    yield
    # Shutdown: stop the scheduler
    await scheduler.stop()

# Create FastAPI app with lifecycle management
app = FastAPI(
    title="Relay",
    description="Agent-Native Data Movement Platform - Where agents move data",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware (for development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount API routes
app.mount("/api/v1", api_router)

# Serve static files (CSS, JS)
static_path = Path(__file__).parent.parent / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# Serve UI
templates_path = Path(__file__).parent.parent / "templates"

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main UI"""
    index_path = templates_path / "index.html"
    if index_path.exists():
        with open(index_path, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    
    # Fallback welcome message
    return HTMLResponse(content="""
    <html>
        <head><title>Relay</title></head>
        <body>
            <h1>Relay - Agent-Native Data Movement</h1>
            <p>Where agents move data.</p>
            <ul>
                <li><a href="/docs">API Documentation</a></li>
                <li><a href="/api/v1/capabilities">API Capabilities</a></li>
            </ul>
        </body>
    </html>
    """)

@app.get("/metadata", response_class=HTMLResponse)
async def metadata_review():
    """Serve metadata review UI"""
    metadata_path = templates_path / "metadata.html"
    if metadata_path.exists():
        with open(metadata_path, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Metadata page not found</h1>", status_code=404)

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "relay", "version": "1.0.0"}

if __name__ == "__main__":
    port = 8001  # Use 8001 since 8000 is taken (Airbyte)
    print("Starting Relay - Agent-Native Data Movement Platform")
    print(f"API: http://localhost:{port}/api/v1")
    print(f"UI: http://localhost:{port}")
    print(f"Docs: http://localhost:{port}/docs")
    uvicorn.run(app, host="0.0.0.0", port=port)
