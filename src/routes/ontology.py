"""
Ontology CRUD routes, semantic query, and proposal management.
"""

import uuid

from fastapi import APIRouter, HTTPException

from ..ontology import OntologyManager
from ..query import QueryEngine
from ..schemas import (
    CreateDimensionRequest,
    CreateEntityRequest,
    CreateMetricRequest,
    CreateRelationshipRequest,
    ProposeOntologyRequest,
    ReviewProposalRequest,
    SemanticQueryRequest,
    UpdateDimensionRequest,
    UpdateEntityRequest,
    UpdateMetricRequest,
)
from ..semantic_query import SemanticQueryEngine
from ..storage import Storage

router = APIRouter(prefix="/ontology", tags=["ontology"])


def _storage() -> Storage:
    return Storage()


def _uuid8() -> str:
    return uuid.uuid4().hex[:8]


# ------------------------------------------------------------------
# Ontology snapshot
# ------------------------------------------------------------------


@router.get("")
async def get_ontology():
    """Full active ontology snapshot."""
    return _storage().get_ontology_snapshot()


# ------------------------------------------------------------------
# Entity CRUD
# ------------------------------------------------------------------


@router.post("/entity")
async def create_entity(req: CreateEntityRequest):
    storage = _storage()

    # Validate pipeline exists
    pipeline = storage.get_pipeline(req.pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=400, detail=f"Pipeline not found: {req.pipeline_id}")

    # Validate unique name
    existing = storage.get_entity_by_name(req.name)
    if existing:
        raise HTTPException(status_code=409, detail=f"Entity with name '{req.name}' already exists")

    entity = {
        "id": f"ent-{_uuid8()}",
        "name": req.name,
        "display_name": req.display_name,
        "description": req.description or "",
        "pipeline_id": req.pipeline_id,
        "column_annotations": req.column_annotations or {},
        "status": "active",
    }
    return storage.save_entity(entity)


@router.get("/entity/list")
async def list_entities(status: str = None):
    return _storage().list_entities(status=status)


@router.get("/entity/{entity_id}")
async def get_entity(entity_id: str):
    entity = _storage().get_entity(entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")
    return entity


@router.get("/entity/by-name/{name}")
async def get_entity_by_name(name: str):
    entity = _storage().get_entity_by_name(name)
    if not entity:
        raise HTTPException(status_code=404, detail=f"Entity '{name}' not found")
    return entity


@router.put("/entity/{entity_id}")
async def update_entity(entity_id: str, req: UpdateEntityRequest):
    updates = req.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")
    result = _storage().update_entity(entity_id, updates)
    if not result:
        raise HTTPException(status_code=404, detail="Entity not found")
    return result


@router.delete("/entity/{entity_id}")
async def delete_entity(entity_id: str):
    if not _storage().delete_entity(entity_id):
        raise HTTPException(status_code=404, detail="Entity not found")
    return {"status": "deleted", "id": entity_id}


# ------------------------------------------------------------------
# Relationship CRUD
# ------------------------------------------------------------------


@router.post("/relationship")
async def create_relationship(req: CreateRelationshipRequest):
    storage = _storage()

    # Validate both entities exist and are active
    from_ent = storage.get_entity_by_name(req.from_entity)
    if not from_ent or from_ent["status"] != "active":
        raise HTTPException(status_code=400, detail=f"Entity '{req.from_entity}' not found or not active")

    to_ent = storage.get_entity_by_name(req.to_entity)
    if not to_ent or to_ent["status"] != "active":
        raise HTTPException(status_code=400, detail=f"Entity '{req.to_entity}' not found or not active")

    rel = {
        "id": f"rel-{_uuid8()}",
        "name": req.name,
        "from_entity": req.from_entity,
        "to_entity": req.to_entity,
        "from_column": req.from_column,
        "to_column": req.to_column,
        "relationship_type": req.relationship_type,
        "description": req.description or "",
    }
    return storage.save_relationship(rel)


@router.get("/relationship/list")
async def list_relationships(entity: str = None):
    return _storage().list_relationships(entity_name=entity)


@router.delete("/relationship/{rel_id}")
async def delete_relationship(rel_id: str):
    if not _storage().delete_relationship(rel_id):
        raise HTTPException(status_code=404, detail="Relationship not found")
    return {"status": "deleted", "id": rel_id}


# ------------------------------------------------------------------
# Metric CRUD
# ------------------------------------------------------------------


@router.post("/metric")
async def create_metric(req: CreateMetricRequest):
    storage = _storage()

    entity = storage.get_entity_by_name(req.entity_name)
    if not entity or entity["status"] != "active":
        raise HTTPException(status_code=400, detail=f"Entity '{req.entity_name}' not found or not active")

    metric = {
        "id": f"met-{_uuid8()}",
        "name": req.name,
        "display_name": req.display_name,
        "description": req.description or "",
        "entity_name": req.entity_name,
        "expression": req.expression,
        "format_type": req.format_type,
    }
    return storage.save_metric(metric)


@router.get("/metric/list")
async def list_metrics(entity: str = None):
    return _storage().list_metrics(entity_name=entity)


@router.put("/metric/{metric_id}")
async def update_metric(metric_id: str, req: UpdateMetricRequest):
    updates = req.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")
    result = _storage().update_metric(metric_id, updates)
    if not result:
        raise HTTPException(status_code=404, detail="Metric not found")
    return result


@router.delete("/metric/{metric_id}")
async def delete_metric(metric_id: str):
    if not _storage().delete_metric(metric_id):
        raise HTTPException(status_code=404, detail="Metric not found")
    return {"status": "deleted", "id": metric_id}


# ------------------------------------------------------------------
# Dimension CRUD
# ------------------------------------------------------------------


@router.post("/dimension")
async def create_dimension(req: CreateDimensionRequest):
    storage = _storage()

    entity = storage.get_entity_by_name(req.entity_name)
    if not entity or entity["status"] != "active":
        raise HTTPException(status_code=400, detail=f"Entity '{req.entity_name}' not found or not active")

    dim = {
        "id": f"dim-{_uuid8()}",
        "name": req.name,
        "display_name": req.display_name,
        "description": req.description or "",
        "entity_name": req.entity_name,
        "expression": req.expression,
        "dimension_type": req.dimension_type,
    }
    return storage.save_dimension(dim)


@router.get("/dimension/list")
async def list_dimensions(entity: str = None):
    return _storage().list_dimensions(entity_name=entity)


@router.put("/dimension/{dim_id}")
async def update_dimension(dim_id: str, req: UpdateDimensionRequest):
    updates = req.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")
    result = _storage().update_dimension(dim_id, updates)
    if not result:
        raise HTTPException(status_code=404, detail="Dimension not found")
    return result


@router.delete("/dimension/{dim_id}")
async def delete_dimension(dim_id: str):
    if not _storage().delete_dimension(dim_id):
        raise HTTPException(status_code=404, detail="Dimension not found")
    return {"status": "deleted", "id": dim_id}


# ------------------------------------------------------------------
# Semantic Query
# ------------------------------------------------------------------


@router.post("/query")
async def semantic_query(req: SemanticQueryRequest):
    storage = _storage()
    query_engine = QueryEngine(storage)
    semantic_engine = SemanticQueryEngine(storage, query_engine)

    try:
        return semantic_engine.execute(req.model_dump(exclude_none=True))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ------------------------------------------------------------------
# Propose & Review
# ------------------------------------------------------------------


@router.post("/propose")
async def propose_ontology(req: ProposeOntologyRequest):
    storage = _storage()
    manager = OntologyManager(storage)

    try:
        proposals = manager.propose_for_pipeline(
            req.pipeline_id,
            include_relationships=req.include_relationships,
            include_metrics=req.include_metrics,
        )
        return {"proposals": proposals, "count": len(proposals)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/proposal/list")
async def list_proposals(status: str = None, type: str = None):
    return _storage().list_proposals(status=status, proposal_type=type)


@router.get("/proposal/{proposal_id}")
async def get_proposal(proposal_id: str):
    proposal = _storage().get_proposal(proposal_id)
    if not proposal:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return proposal


@router.post("/proposal/{proposal_id}/review")
async def review_proposal(proposal_id: str, req: ReviewProposalRequest):
    storage = _storage()
    manager = OntologyManager(storage)

    try:
        if req.action == "approve":
            return manager.approve_proposal(proposal_id, reviewed_by="user")
        elif req.action == "reject":
            return manager.reject_proposal(proposal_id, reviewed_by="user", notes=req.notes)
        else:
            raise HTTPException(status_code=400, detail="action must be 'approve' or 'reject'")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
