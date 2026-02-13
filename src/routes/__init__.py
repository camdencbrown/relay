"""
Relay API routes
"""

from fastapi import APIRouter

from .capabilities import router as capabilities_router
from .metadata import router as metadata_router
from .pipelines import router as pipelines_router
from .query import router as query_router
from .search import router as search_router
from .transformations import router as transformations_router

api_router = APIRouter()
api_router.include_router(capabilities_router)
api_router.include_router(pipelines_router)
api_router.include_router(query_router)
api_router.include_router(metadata_router)
api_router.include_router(search_router)
api_router.include_router(transformations_router)
