"""
Dataset search and join suggestion endpoints
"""

from fastapi import APIRouter

from ..dataset_search import DatasetSearch
from ..storage import Storage

router = APIRouter()

_storage = Storage()
_dataset_search = DatasetSearch(_storage)


@router.get("/datasets/search")
async def search_datasets(q: str, top_k: int = 5):
    results = _dataset_search.search(q, top_k)
    return {
        "status": "success",
        "query": q,
        "results_count": len(results),
        "results": results,
        "next_steps": "Use pipeline_id to get metadata or create transformation",
    }


@router.get("/datasets/join-suggestions")
async def get_join_suggestions(dataset1: str, dataset2: str):
    suggestions = _dataset_search.get_join_suggestions(dataset1, dataset2)
    return {
        "status": "success",
        "dataset1": dataset1,
        "dataset2": dataset2,
        "suggestions_count": len(suggestions),
        "suggestions": suggestions,
        "next_steps": "Use suggested join keys in transformation pipeline",
    }
