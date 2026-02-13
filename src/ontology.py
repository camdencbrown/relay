"""
Ontology Manager for Relay
AI-driven ontology building: proposes entities, relationships, metrics, dimensions
from pipeline data. Supports heuristic fallback when Claude is unavailable.
"""

import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .config import get_settings
from .storage import Storage

logger = logging.getLogger(__name__)


def _uuid8() -> str:
    return uuid.uuid4().hex[:8]


class OntologyManager:
    """Manages AI-driven ontology building and approval workflow."""

    def __init__(self, storage: Storage):
        self.storage = storage

    def propose_for_pipeline(
        self,
        pipeline_id: str,
        include_relationships: bool = True,
        include_metrics: bool = True,
    ) -> List[Dict]:
        """Analyze a pipeline and generate ontology proposals."""
        pipeline = self.storage.get_pipeline(pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_id}")

        metadata = self.storage.get_metadata(pipeline_id)
        existing_entities = self.storage.list_entities(status="active")

        # Try AI analysis, fall back to heuristics
        settings = get_settings()
        if settings.anthropic_api_key:
            proposals = self._ai_propose(pipeline, metadata, existing_entities, include_relationships, include_metrics)
        else:
            proposals = self._heuristic_propose(
                pipeline, metadata, existing_entities, include_relationships, include_metrics
            )

        # Save proposals and optionally auto-approve
        auto_approve = not settings.require_auth
        saved = []
        for prop in proposals:
            prop_record = {
                "id": f"prop-{_uuid8()}",
                "proposal_type": prop["type"],
                "payload": prop["payload"],
                "source_pipeline_id": pipeline_id,
                "proposed_by": "ai" if settings.anthropic_api_key else "heuristic",
                "status": "approved" if auto_approve else "pending",
            }
            saved_prop = self.storage.save_proposal(prop_record)

            if auto_approve:
                self._materialize_proposal(prop)

            saved.append(saved_prop)

        return saved

    def approve_proposal(self, proposal_id: str, reviewed_by: str = "user") -> Dict:
        """Approve a proposal and create the ontology object."""
        proposal = self.storage.get_proposal(proposal_id)
        if not proposal:
            raise ValueError(f"Proposal not found: {proposal_id}")
        if proposal["status"] != "pending":
            raise ValueError(f"Proposal is not pending: {proposal['status']}")

        now = datetime.now(timezone.utc).isoformat()
        self.storage.update_proposal(
            proposal_id,
            {"status": "approved", "reviewed_by": reviewed_by, "reviewed_at": now},
        )

        created = self._materialize_proposal(
            {"type": proposal["proposal_type"], "payload": proposal["payload"]}
        )
        return {"proposal_id": proposal_id, "status": "approved", "created": created}

    def reject_proposal(
        self, proposal_id: str, reviewed_by: str = "user", notes: str = None
    ) -> Dict:
        """Reject a proposal."""
        proposal = self.storage.get_proposal(proposal_id)
        if not proposal:
            raise ValueError(f"Proposal not found: {proposal_id}")
        if proposal["status"] != "pending":
            raise ValueError(f"Proposal is not pending: {proposal['status']}")

        now = datetime.now(timezone.utc).isoformat()
        self.storage.update_proposal(
            proposal_id,
            {
                "status": "rejected",
                "reviewed_by": reviewed_by,
                "reviewed_at": now,
                "review_notes": notes,
            },
        )
        return {"proposal_id": proposal_id, "status": "rejected"}

    def _materialize_proposal(self, proposal: Dict) -> Dict:
        """Create the ontology object from a proposal payload."""
        ptype = proposal["type"]
        payload = proposal["payload"]

        if ptype == "entity":
            payload.setdefault("id", f"ent-{_uuid8()}")
            return self.storage.save_entity(payload)
        elif ptype == "relationship":
            payload.setdefault("id", f"rel-{_uuid8()}")
            return self.storage.save_relationship(payload)
        elif ptype == "metric":
            payload.setdefault("id", f"met-{_uuid8()}")
            return self.storage.save_metric(payload)
        elif ptype == "dimension":
            payload.setdefault("id", f"dim-{_uuid8()}")
            return self.storage.save_dimension(payload)
        else:
            raise ValueError(f"Unknown proposal type: {ptype}")

    # ------------------------------------------------------------------
    # Heuristic proposals (no Claude needed)
    # ------------------------------------------------------------------

    def _heuristic_propose(
        self,
        pipeline: Dict,
        metadata: Optional[Dict],
        existing_entities: List[Dict],
        include_relationships: bool,
        include_metrics: bool,
    ) -> List[Dict]:
        proposals = []
        pipeline_name = pipeline["name"]
        entity_name = self._normalize_entity_name(pipeline_name)
        columns = metadata.get("columns", []) if metadata else []

        # Propose entity
        column_annotations = {}
        for col in columns:
            name = col["name"]
            if name == "id" or name.endswith("_id") and name == "id":
                column_annotations[name] = {"role": "primary_key", "description": col.get("description", "")}
            elif col.get("semantic_type") == "identifier":
                column_annotations[name] = {"role": "primary_key", "description": col.get("description", "")}

        proposals.append(
            {
                "type": "entity",
                "payload": {
                    "name": entity_name,
                    "display_name": pipeline_name,
                    "description": pipeline.get("description", f"Entity from pipeline '{pipeline_name}'"),
                    "pipeline_id": pipeline["id"],
                    "column_annotations": column_annotations,
                    "status": "active",
                    "proposed_by": "heuristic",
                },
            }
        )

        # Propose relationships based on *_id columns
        if include_relationships:
            existing_names = {e["name"] for e in existing_entities}
            for col in columns:
                col_name = col["name"]
                if col_name.endswith("_id") and col_name != "id":
                    ref_entity = col_name[:-3]  # e.g. "customer_id" -> "customer"
                    # Check singular and plural forms
                    for candidate in [ref_entity, ref_entity + "s"]:
                        if candidate in existing_names:
                            proposals.append(
                                {
                                    "type": "relationship",
                                    "payload": {
                                        "name": f"{entity_name}_to_{candidate}",
                                        "from_entity": entity_name,
                                        "to_entity": candidate,
                                        "from_column": col_name,
                                        "to_column": "id",
                                        "relationship_type": "many_to_one",
                                        "description": f"{entity_name}.{col_name} -> {candidate}.id",
                                        "status": "active",
                                        "proposed_by": "heuristic",
                                    },
                                }
                            )
                            break

        # Propose metrics and dimensions
        if include_metrics:
            for col in columns:
                col_name = col["name"]
                col_type = col.get("type", "")
                semantic = col.get("semantic_type", "")

                # Numeric columns -> SUM/AVG metrics
                if col_type in ("int64", "float64", "numeric", "integer", "float") or semantic in (
                    "currency",
                    "numeric",
                    "amount",
                ):
                    if col_name != "id" and not col_name.endswith("_id"):
                        proposals.append(
                            {
                                "type": "metric",
                                "payload": {
                                    "name": f"total_{col_name}",
                                    "display_name": f"Total {col_name.replace('_', ' ').title()}",
                                    "description": f"Sum of {entity_name}.{col_name}",
                                    "entity_name": entity_name,
                                    "expression": f"SUM({entity_name}.{col_name})",
                                    "format_type": "currency" if semantic == "currency" else "number",
                                    "status": "active",
                                    "proposed_by": "heuristic",
                                },
                            }
                        )
                        proposals.append(
                            {
                                "type": "metric",
                                "payload": {
                                    "name": f"avg_{col_name}",
                                    "display_name": f"Average {col_name.replace('_', ' ').title()}",
                                    "description": f"Average of {entity_name}.{col_name}",
                                    "entity_name": entity_name,
                                    "expression": f"AVG({entity_name}.{col_name})",
                                    "format_type": "number",
                                    "status": "active",
                                    "proposed_by": "heuristic",
                                },
                            }
                        )

                # Count metric for the entity
            proposals.append(
                {
                    "type": "metric",
                    "payload": {
                        "name": f"{entity_name}_count",
                        "display_name": f"{pipeline_name} Count",
                        "description": f"Count of {entity_name} records",
                        "entity_name": entity_name,
                        "expression": "COUNT(*)",
                        "format_type": "number",
                        "status": "active",
                        "proposed_by": "heuristic",
                    },
                }
            )

            # Dimensions: low-cardinality text + datetime columns
            for col in columns:
                col_name = col["name"]
                col_type = col.get("type", "")
                semantic = col.get("semantic_type", "")
                unique_vals = col.get("unique_values", 0)

                if semantic in ("date", "datetime") or "date" in col_type.lower():
                    proposals.append(
                        {
                            "type": "dimension",
                            "payload": {
                                "name": f"{col_name}_month",
                                "display_name": f"{col_name.replace('_', ' ').title()} (Month)",
                                "description": f"Monthly grouping of {entity_name}.{col_name}",
                                "entity_name": entity_name,
                                "expression": f"DATE_TRUNC('month', {entity_name}.{col_name})",
                                "dimension_type": "derived",
                                "status": "active",
                                "proposed_by": "heuristic",
                            },
                        }
                    )
                elif (
                    col_type in ("object", "string", "text", "category")
                    and 0 < unique_vals <= 50
                    and col_name != "id"
                    and not col_name.endswith("_id")
                ):
                    proposals.append(
                        {
                            "type": "dimension",
                            "payload": {
                                "name": col_name,
                                "display_name": col_name.replace("_", " ").title(),
                                "description": f"Group by {entity_name}.{col_name}",
                                "entity_name": entity_name,
                                "expression": f"{entity_name}.{col_name}",
                                "dimension_type": "direct",
                                "status": "active",
                                "proposed_by": "heuristic",
                            },
                        }
                    )

        return proposals

    # ------------------------------------------------------------------
    # AI-powered proposals
    # ------------------------------------------------------------------

    def _ai_propose(
        self,
        pipeline: Dict,
        metadata: Optional[Dict],
        existing_entities: List[Dict],
        include_relationships: bool,
        include_metrics: bool,
    ) -> List[Dict]:
        """Use Claude to analyze pipeline and propose ontology elements."""
        try:
            import anthropic

            settings = get_settings()
            client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

            prompt = self._build_ai_prompt(pipeline, metadata, existing_entities, include_relationships, include_metrics)
            message = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
            )
            response_text = message.content[0].text

            parsed = self._parse_ai_response(response_text)
            if parsed:
                return parsed

        except Exception as e:
            logger.warning(f"AI proposal failed, falling back to heuristics: {e}")

        return self._heuristic_propose(pipeline, metadata, existing_entities, include_relationships, include_metrics)

    def _build_ai_prompt(
        self,
        pipeline: Dict,
        metadata: Optional[Dict],
        existing_entities: List[Dict],
        include_relationships: bool,
        include_metrics: bool,
    ) -> str:
        columns_info = []
        if metadata:
            for col in metadata.get("columns", []):
                columns_info.append(
                    {
                        "name": col["name"],
                        "type": col.get("type", "unknown"),
                        "semantic_type": col.get("semantic_type", "unknown"),
                        "sample_values": col.get("sample_values", [])[:5],
                        "null_percentage": col.get("null_percentage", 0),
                        "unique_values": col.get("unique_values", 0),
                    }
                )

        existing = [{"name": e["name"], "pipeline_id": e["pipeline_id"]} for e in existing_entities]

        sections = [
            f"Pipeline: {pipeline['name']} (id: {pipeline['id']})",
            f"Columns: {json.dumps(columns_info, indent=2)}",
            f"Existing entities: {json.dumps(existing)}",
        ]

        request_parts = ["entity (name, display_name, description, column_annotations)"]
        if include_relationships:
            request_parts.append(
                "relationships (name, from_entity, to_entity, from_column, to_column, relationship_type)"
            )
        if include_metrics:
            request_parts.append("metrics (name, display_name, expression using entity_name.column, format_type)")
            request_parts.append(
                "dimensions (name, display_name, expression using entity_name.column, dimension_type: direct|derived)"
            )

        return (
            "Analyze this pipeline data and propose ontology elements.\n\n"
            + "\n".join(sections)
            + "\n\n"
            + f"Propose: {', '.join(request_parts)}\n\n"
            + "Respond ONLY with a JSON array of objects, each with 'type' (entity/relationship/metric/dimension) "
            + "and 'payload' containing the fields for that type. "
            + "Use the pipeline name (normalized to lowercase/underscores) as the entity name. "
            + "Metric/dimension expressions should use entity_name.column_name format."
        )

    @staticmethod
    def _parse_ai_response(text: str) -> Optional[List[Dict]]:
        """Parse AI response as JSON array of proposals."""
        try:
            result = json.loads(text)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

        match = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", text, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group(1))
                if isinstance(result, list):
                    return result
            except json.JSONDecodeError:
                pass

        return None

    @staticmethod
    def _normalize_entity_name(name: str) -> str:
        """Convert pipeline name to entity name: lowercase, underscores, no special chars."""
        result = name.lower().strip().replace(" ", "_").replace("-", "_")
        result = re.sub(r"[^a-z0-9_]", "", result)
        return result
