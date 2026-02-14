"""
Semantic Query Engine for Relay
Resolves ontology metrics/dimensions into SQL, executes via QueryEngine.
"""

import json
import logging
import re
from collections import deque
from typing import Dict, List, Optional, Set, Tuple

from .config import get_settings
from .storage import Storage
from .utils import sanitize_table_name

logger = logging.getLogger(__name__)


class SemanticQueryEngine:
    """Translates semantic queries (metrics + dimensions) into SQL over pipeline data."""

    def __init__(self, storage: Storage, query_engine):
        self.storage = storage
        self.query_engine = query_engine

    def execute(self, request: Dict) -> Dict:
        """Main entry: routes to structured or NL path."""
        if request.get("natural_language"):
            return self._resolve_natural_language(request["natural_language"])
        return self._resolve_structured(
            metrics=request.get("metrics", []),
            dimensions=request.get("dimensions", []),
            filters=request.get("filters", []),
            order_by=request.get("order_by", []),
            limit=request.get("limit"),
        )

    def _resolve_structured(
        self,
        metrics: List[str],
        dimensions: List[str],
        filters: List[str],
        order_by: List[str],
        limit: Optional[int],
    ) -> Dict:
        """Resolve structured semantic query to SQL and execute."""
        ontology = self.storage.get_ontology_snapshot()
        entities_by_name = {e["name"]: e for e in ontology["entities"]}
        metrics_by_name = {m["name"]: m for m in ontology["metrics"]}
        dimensions_by_name = {d["name"]: d for d in ontology["dimensions"]}
        relationships = ontology["relationships"]

        # Collect needed entities and resolved expressions
        needed_entities: Set[str] = set()
        select_parts = []
        group_by_parts = []

        # Resolve dimensions
        for dim_name in dimensions:
            expr, entity = self._resolve_dimension(dim_name, dimensions_by_name)
            needed_entities.add(entity)
            select_parts.append(f"{expr} AS {dim_name}")
            group_by_parts.append(expr)

        # Resolve metrics
        for met_name in metrics:
            expr, entity = self._resolve_metric(met_name, metrics_by_name)
            needed_entities.add(entity)
            select_parts.append(f"{expr} AS {met_name}")

        if not select_parts:
            raise ValueError("At least one metric or dimension is required")

        # Build entity -> pipeline -> table alias mapping
        entity_table_map = self._build_entity_table_map(needed_entities, entities_by_name)

        # Build FROM/JOIN clause
        from_clause = self._build_join_graph(needed_entities, relationships, entity_table_map, entities_by_name)

        # Replace entity names with table aliases in all expressions
        select_resolved = [self._substitute_aliases(s, entity_table_map) for s in select_parts]
        group_by_resolved = [self._substitute_aliases(g, entity_table_map) for g in group_by_parts]
        filters_resolved = [self._substitute_aliases(f, entity_table_map) for f in filters]
        order_by_resolved = [self._substitute_aliases(o, entity_table_map) for o in order_by]

        # Build SQL
        sql = f"SELECT {', '.join(select_resolved)}"
        sql += f" FROM {from_clause}"
        if filters_resolved:
            sql += f" WHERE {' AND '.join(filters_resolved)}"
        if group_by_resolved:
            sql += f" GROUP BY {', '.join(group_by_resolved)}"
        if order_by_resolved:
            sql += f" ORDER BY {', '.join(order_by_resolved)}"
        if limit:
            sql += f" LIMIT {limit}"

        # Collect pipeline IDs needed
        pipeline_ids = []
        for ent_name in needed_entities:
            if ent_name in entities_by_name:
                pid = entities_by_name[ent_name]["pipeline_id"]
                if pid not in pipeline_ids:
                    pipeline_ids.append(pid)

        # Execute via QueryEngine
        result = self.query_engine.execute_query(pipeline_ids, sql, limit=limit or 1000)
        result["generated_sql"] = sql
        result["entities_used"] = list(needed_entities)
        return result

    def _resolve_natural_language(self, question: str) -> Dict:
        """Use Claude to convert NL to structured semantic query, then execute."""
        settings = get_settings()
        if not settings.anthropic_api_key:
            raise ValueError(
                "Natural language queries require ANTHROPIC_API_KEY. "
                "Use structured queries (metrics + dimensions) instead."
            )

        ontology = self.storage.get_ontology_snapshot()
        prompt = self._build_nl_prompt(question, ontology)

        try:
            import anthropic

            client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
            message = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            response_text = message.content[0].text

            # Parse JSON from response
            parsed = self._parse_json_response(response_text)
            if not parsed:
                raise ValueError("Could not parse structured query from AI response")

            result = self._resolve_structured(
                metrics=parsed.get("metrics", []),
                dimensions=parsed.get("dimensions", []),
                filters=parsed.get("filters", []),
                order_by=parsed.get("order_by", []),
                limit=parsed.get("limit"),
            )
            result["natural_language_query"] = question
            result["explanation"] = parsed.get("explanation", "")
            return result

        except ImportError:
            raise ValueError("anthropic package required for NL queries")

    def _build_nl_prompt(self, question: str, ontology: Dict) -> str:
        entities = [{"name": e["name"], "description": e["description"]} for e in ontology["entities"]]
        metrics_list = [
            {"name": m["name"], "expression": m["expression"], "entity": m["entity_name"]}
            for m in ontology["metrics"]
        ]
        dims_list = [
            {"name": d["name"], "expression": d["expression"], "entity": d["entity_name"]}
            for d in ontology["dimensions"]
        ]
        rels = [
            {
                "from": r["from_entity"],
                "to": r["to_entity"],
                "on": f"{r['from_entity']}.{r['from_column']} = {r['to_entity']}.{r['to_column']}",
            }
            for r in ontology["relationships"]
        ]

        return (
            "Convert this question into a structured semantic query.\n\n"
            f"Question: {question}\n\n"
            f"Available entities: {json.dumps(entities)}\n"
            f"Available metrics: {json.dumps(metrics_list)}\n"
            f"Available dimensions: {json.dumps(dims_list)}\n"
            f"Available relationships: {json.dumps(rels)}\n\n"
            "Respond ONLY with valid JSON:\n"
            '{"metrics": [...], "dimensions": [...], "filters": [...], "order_by": [...], "limit": N, "explanation": "..."}\n'
            "Use only metric/dimension names from the lists above."
        )

    def _resolve_metric(self, name: str, metrics_by_name: Dict, _seen: Set[str] = None) -> Tuple[str, str]:
        """Resolve a metric name to (expression, entity_name). Supports ${ref} composability."""
        if _seen is None:
            _seen = set()
        if name in _seen:
            raise ValueError(f"Circular metric reference detected: {name}")
        _seen.add(name)

        metric = metrics_by_name.get(name)
        if not metric:
            raise ValueError(f"Unknown metric: {name}")

        expr = metric["expression"]
        # Resolve ${other_metric} references
        for ref_match in re.finditer(r"\$\{(\w+)\}", expr):
            ref_name = ref_match.group(1)
            ref_expr, _ = self._resolve_metric(ref_name, metrics_by_name, _seen)
            expr = expr.replace(ref_match.group(0), f"({ref_expr})")

        return expr, metric["entity_name"]

    def _resolve_dimension(self, name: str, dimensions_by_name: Dict) -> Tuple[str, str]:
        """Resolve a dimension name to (expression, entity_name)."""
        dim = dimensions_by_name.get(name)
        if not dim:
            raise ValueError(f"Unknown dimension: {name}")
        return dim["expression"], dim["entity_name"]

    def _build_entity_table_map(self, entity_names: Set[str], entities_by_name: Dict) -> Dict[str, str]:
        """Map entity names -> table aliases via pipeline names."""
        mapping = {}
        for ent_name in entity_names:
            entity = entities_by_name.get(ent_name)
            if not entity:
                raise ValueError(f"Unknown entity: {ent_name}")
            pipeline = self.storage.get_pipeline(entity["pipeline_id"])
            if not pipeline:
                raise ValueError(f"Pipeline not found for entity '{ent_name}': {entity['pipeline_id']}")
            mapping[ent_name] = sanitize_table_name(pipeline["name"])
        return mapping

    def _build_join_graph(
        self,
        entity_names: Set[str],
        relationships: List[Dict],
        entity_table_map: Dict[str, str],
        entities_by_name: Dict,
    ) -> str:
        """BFS from first entity, build FROM/JOIN chain."""
        if not entity_names:
            raise ValueError("No entities to query")

        entities = list(entity_names)
        if len(entities) == 1:
            return entity_table_map[entities[0]]

        # Build adjacency map from relationships
        adj: Dict[str, List[Dict]] = {e: [] for e in entities}
        for rel in relationships:
            if rel["from_entity"] in entity_names and rel["to_entity"] in entity_names:
                adj[rel["from_entity"]].append(rel)
                # Add reverse edge for BFS traversal
                adj[rel["to_entity"]].append(rel)

        # BFS from first entity
        root = entities[0]
        visited = {root}
        queue = deque([root])
        join_clauses = []

        while queue:
            current = queue.popleft()
            for rel in adj.get(current, []):
                # Determine which side is the unvisited entity
                if rel["from_entity"] == current and rel["to_entity"] not in visited:
                    next_ent = rel["to_entity"]
                    from_alias = entity_table_map[rel["from_entity"]]
                    to_alias = entity_table_map[rel["to_entity"]]
                    on_clause = f"{from_alias}.{rel['from_column']} = {to_alias}.{rel['to_column']}"
                    join_clauses.append(f"LEFT JOIN {to_alias} ON {on_clause}")
                    visited.add(next_ent)
                    queue.append(next_ent)
                elif rel["to_entity"] == current and rel["from_entity"] not in visited:
                    next_ent = rel["from_entity"]
                    from_alias = entity_table_map[rel["from_entity"]]
                    to_alias = entity_table_map[rel["to_entity"]]
                    on_clause = f"{from_alias}.{rel['from_column']} = {to_alias}.{rel['to_column']}"
                    join_clauses.append(f"LEFT JOIN {from_alias} ON {on_clause}")
                    visited.add(next_ent)
                    queue.append(next_ent)

        from_clause = entity_table_map[root]
        if join_clauses:
            from_clause += " " + " ".join(join_clauses)

        return from_clause

    def _substitute_aliases(self, expr: str, entity_table_map: Dict[str, str]) -> str:
        """Replace entity_name.column with table_alias.column in expressions."""
        for entity_name, table_alias in entity_table_map.items():
            # Replace entity_name. with table_alias. (word boundary)
            expr = re.sub(rf"\b{re.escape(entity_name)}\.", f"{table_alias}.", expr)
        return expr

    @staticmethod
    def _parse_json_response(text: str) -> Optional[Dict]:
        """Parse JSON from raw text or markdown code block."""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except json.JSONDecodeError:
                    pass
            return None
