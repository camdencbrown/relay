"""
Data lineage computation for Relay.
Computes entity→pipeline→source traceability from existing data (no new table).
"""

import re
from typing import Dict, List, Optional


def extract_column_references(expression: str) -> List[str]:
    """Extract table.column references from a SQL expression.

    >>> extract_column_references("SUM(orders.total)")
    ['orders.total']
    >>> extract_column_references("COUNT(*)")
    []
    """
    return re.findall(r"\b(\w+\.\w+)\b", expression)


def compute_lineage(entity_name: str, storage) -> Optional[Dict]:
    """Compute full lineage for an entity.

    Returns entity info, pipeline info, source config, metrics/dimensions
    with column references, and relationship graph.
    """
    entity = storage.get_entity_by_name(entity_name)
    if not entity:
        return None

    # Pipeline info
    pipeline = storage.get_pipeline(entity["pipeline_id"])
    pipeline_info = None
    source_info = None
    if pipeline:
        pipeline_info = {
            "id": pipeline["id"],
            "name": pipeline["name"],
            "type": pipeline.get("type", "regular"),
            "status": pipeline.get("status"),
        }
        source_info = pipeline.get("source", {})

    # Metrics with column references
    metrics = storage.list_metrics(entity_name=entity_name, status="active")
    for m in metrics:
        m["column_references"] = extract_column_references(m.get("expression", ""))

    # Dimensions with column references
    dimensions = storage.list_dimensions(entity_name=entity_name, status="active")
    for d in dimensions:
        d["column_references"] = extract_column_references(d.get("expression", ""))

    # Relationships
    relationships = storage.list_relationships(entity_name=entity_name, status="active")
    outgoing = [r for r in relationships if r["from_entity"] == entity_name]
    incoming = [r for r in relationships if r["to_entity"] == entity_name]

    downstream_entities = list({r["to_entity"] for r in outgoing})
    upstream_entities = list({r["from_entity"] for r in incoming})

    return {
        "entity": entity,
        "pipeline": pipeline_info,
        "source": source_info,
        "metrics": metrics,
        "dimensions": dimensions,
        "relationships": {
            "outgoing": outgoing,
            "incoming": incoming,
        },
        "downstream_entities": downstream_entities,
        "upstream_entities": upstream_entities,
    }
