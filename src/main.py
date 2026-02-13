"""
Relay - Agent-Native Data Movement Platform
Main FastAPI application
"""

from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .config import get_settings
from .database import init_db
from .logging_config import setup_logging
from .pipeline import PipelineEngine
from .routes import api_router
from .scheduler import PipelineScheduler
from .storage import Storage

# Initialize components
storage = Storage()
engine = PipelineEngine(storage)
scheduler = PipelineScheduler(storage, engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    init_db()
    await scheduler.start()
    yield
    await scheduler.stop()


settings = get_settings()

app = FastAPI(
    title="Relay",
    description="Agent-Native Data Movement Platform",
    version=settings.app_version,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount API routes
app.mount("/api/v1", api_router)

# Serve static files
static_path = Path(__file__).parent.parent / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

templates_path = Path(__file__).parent.parent / "templates"


@app.get("/", response_class=HTMLResponse)
async def root():
    index_path = templates_path / "index.html"
    if index_path.exists():
        with open(index_path, encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(
        content="<h1>Relay</h1><p>Agent-Native Data Movement Platform</p>"
        '<ul><li><a href="/docs">API Docs</a></li>'
        '<li><a href="/api/v1/capabilities">Capabilities</a></li></ul>'
    )


@app.get("/metadata", response_class=HTMLResponse)
async def metadata_review():
    metadata_path = templates_path / "metadata.html"
    if metadata_path.exists():
        with open(metadata_path, encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Metadata page not found</h1>", status_code=404)


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "relay",
        "version": settings.app_version,
        "components": {
            "database": "sqlite",
            "query_engine": "duckdb",
            "storage": "s3",
        },
    }


if __name__ == "__main__":
    print(f"Starting Relay v{settings.app_version}")
    print(f"API: http://localhost:{settings.port}/api/v1")
    print(f"UI: http://localhost:{settings.port}")
    uvicorn.run(app, host=settings.host, port=settings.port)
