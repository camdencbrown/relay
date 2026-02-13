#!/usr/bin/env python3
"""
Relay Ontology Layer - End-to-End Demo Script

Walks through the full ontology workflow against a running Relay server:
  1. Create pipelines (Orders, Customers) with synthetic data
  2. Register entities in the ontology
  3. Create relationships, metrics, and dimensions
  4. View the full ontology snapshot
  5. Run a structured semantic query
  6. Demonstrate the proposal workflow
  7. Show the capabilities endpoint

Requirements:
  - A running Relay server at http://localhost:8001
  - `pip install requests`
  - No S3/AWS credentials needed (this demo uses only CRUD + proposal operations)

Usage:
  python demo_ontology.py
"""

import json
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE = "http://localhost:8001"
API = f"{BASE}/api/v1"

# ---------------------------------------------------------------------------
# ANSI colour helpers
# ---------------------------------------------------------------------------
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"
BLUE = "\033[34m"
WHITE = "\033[97m"
BG_BLUE = "\033[44m"
BG_GREEN = "\033[42m"
BG_RED = "\033[41m"
BG_YELLOW = "\033[43m"

# Track created IDs so we can reference them across steps
state = {
    "orders_pipeline_id": None,
    "customers_pipeline_id": None,
    "orders_entity_id": None,
    "customers_entity_id": None,
    "relationship_id": None,
    "metric_ids": [],
    "dimension_ids": [],
    "proposal_ids": [],
}


# ---------------------------------------------------------------------------
# Printing helpers
# ---------------------------------------------------------------------------
def banner():
    print()
    print(f"{BG_BLUE}{WHITE}{BOLD}")
    print("  ====================================================================")
    print("    RELAY ONTOLOGY LAYER  --  End-to-End Demo")
    print("  ====================================================================")
    print(f"  {RESET}")
    print()
    print(f"  {DIM}This script demonstrates the full ontology workflow:{RESET}")
    print(f"    {CYAN}1.{RESET} Create pipelines (synthetic data sources)")
    print(f"    {CYAN}2.{RESET} Register entities in the ontology")
    print(f"    {CYAN}3.{RESET} Define relationships between entities")
    print(f"    {CYAN}4.{RESET} Create business metrics (revenue, order count)")
    print(f"    {CYAN}5.{RESET} Create dimensions (customer segment, region)")
    print(f"    {CYAN}6.{RESET} View the complete ontology snapshot")
    print(f"    {CYAN}7.{RESET} Execute a structured semantic query")
    print(f"    {CYAN}8.{RESET} Demonstrate the proposal workflow")
    print(f"    {CYAN}9.{RESET} Show capabilities endpoint")
    print()
    print(f"  {DIM}Target server: {BASE}{RESET}")
    print()


def step(number, title):
    print()
    print(f"  {BG_GREEN}{WHITE}{BOLD} STEP {number} {RESET}  {BOLD}{title}{RESET}")
    print(f"  {'=' * 60}")


def substep(label):
    print(f"\n  {CYAN}{BOLD}>> {label}{RESET}")


def show_request(method, url, body=None):
    print(f"  {DIM}{method} {url}{RESET}")
    if body:
        print(f"  {DIM}Body:{RESET}")
        for line in json.dumps(body, indent=2).splitlines():
            print(f"    {DIM}{line}{RESET}")


def show_response(resp, label="Response"):
    status_color = GREEN if resp.status_code < 400 else RED
    print(f"  {status_color}{BOLD}{resp.status_code}{RESET} {label}")
    try:
        data = resp.json()
        formatted = json.dumps(data, indent=2, default=str)
        for line in formatted.splitlines():
            print(f"    {line}")
    except Exception:
        print(f"    {resp.text[:500]}")
    return resp


def success(msg):
    print(f"  {GREEN}{BOLD}[OK]{RESET} {msg}")


def warn(msg):
    print(f"  {YELLOW}{BOLD}[WARN]{RESET} {msg}")


def error(msg):
    print(f"  {RED}{BOLD}[ERROR]{RESET} {msg}")


def info(msg):
    print(f"  {BLUE}{BOLD}[INFO]{RESET} {msg}")


def separator():
    print(f"\n  {DIM}{'- ' * 30}{RESET}")


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------
def api_get(path, **kwargs):
    url = f"{API}{path}"
    show_request("GET", url)
    resp = requests.get(url, timeout=15, **kwargs)
    show_response(resp)
    return resp


def api_post(path, body=None, **kwargs):
    url = f"{API}{path}"
    show_request("POST", url, body)
    resp = requests.post(url, json=body, timeout=15, **kwargs)
    show_response(resp)
    return resp


def api_delete(path, **kwargs):
    url = f"{API}{path}"
    show_request("DELETE", url)
    resp = requests.delete(url, timeout=15, **kwargs)
    show_response(resp)
    return resp


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
def check_health():
    print(f"  {DIM}Checking server health...{RESET}")
    try:
        resp = requests.get(f"{BASE}/health", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            success(f"Server is {data['status']} (v{data.get('version', '?')})")
            return True
        else:
            error(f"Unexpected status: {resp.status_code}")
            return False
    except requests.ConnectionError:
        error(f"Cannot connect to {BASE}")
        print()
        print(f"  {YELLOW}Make sure the Relay server is running:{RESET}")
        print(f"    cd relay && python -m src.main")
        print(f"  {YELLOW}Or:{RESET}")
        print(f"    uvicorn src.main:app --port 8001 --reload")
        print()
        return False
    except Exception as e:
        error(f"Health check failed: {e}")
        return False


# ===================================================================
# DEMO STEPS
# ===================================================================

def step_1_create_pipelines():
    """Create Orders and Customers pipelines with synthetic data."""
    step(1, "Create Pipelines (Synthetic Data Sources)")

    info("Creating two pipelines with synthetic column schemas.")
    info("These define the data shape -- no S3/execution needed for ontology CRUD.")

    # ---- Orders pipeline ----
    substep("Create 'Orders' pipeline")
    orders_body = {
        "name": "Orders",
        "description": "E-commerce order transactions",
        "source": {
            "type": "synthetic",
            "url": "synthetic://orders",
            "schema": {
                "id": "uuid",
                "customer_id": "uuid",
                "total": "currency",
                "quantity": "integer:1:20",
                "status": "string:8",
                "created_at": "date",
            },
            "row_count": 500,
        },
        "destination": {
            "type": "s3",
            "bucket": "relay-demo",
            "path": "orders/",
        },
    }
    resp = api_post("/pipeline/create", orders_body)
    if resp.status_code == 200:
        state["orders_pipeline_id"] = resp.json()["pipeline_id"]
        success(f"Orders pipeline created: {state['orders_pipeline_id']}")
    else:
        error("Failed to create Orders pipeline")
        return False

    separator()

    # ---- Customers pipeline ----
    substep("Create 'Customers' pipeline")
    customers_body = {
        "name": "Customers",
        "description": "Customer master data with segmentation",
        "source": {
            "type": "synthetic",
            "url": "synthetic://customers",
            "schema": {
                "id": "uuid",
                "name": "first_name",
                "email": "email",
                "segment": "string:8",
                "region": "country",
            },
            "row_count": 200,
        },
        "destination": {
            "type": "s3",
            "bucket": "relay-demo",
            "path": "customers/",
        },
    }
    resp = api_post("/pipeline/create", customers_body)
    if resp.status_code == 200:
        state["customers_pipeline_id"] = resp.json()["pipeline_id"]
        success(f"Customers pipeline created: {state['customers_pipeline_id']}")
    else:
        error("Failed to create Customers pipeline")
        return False

    separator()

    substep("List all pipelines")
    api_get("/pipeline/list")
    return True


def step_2_create_entities():
    """Register entities in the ontology, mapped to the pipelines."""
    step(2, "Register Entities in the Ontology")

    info("Entities map a semantic name to a pipeline + column annotations.")

    # ---- Orders entity ----
    substep("Create 'orders' entity")
    orders_entity = {
        "name": "orders",
        "display_name": "Orders",
        "description": "E-commerce order transactions including total, quantity, and status",
        "pipeline_id": state["orders_pipeline_id"],
        "column_annotations": {
            "id": {"role": "primary_key", "description": "Unique order identifier"},
            "customer_id": {"role": "foreign_key", "description": "Reference to customers.id"},
            "total": {"role": "measure", "description": "Order total amount in USD"},
            "quantity": {"role": "measure", "description": "Number of items in order"},
            "status": {"role": "attribute", "description": "Order status code"},
            "created_at": {"role": "timestamp", "description": "Order creation date"},
        },
    }
    resp = api_post("/ontology/entity", orders_entity)
    if resp.status_code == 200:
        state["orders_entity_id"] = resp.json()["id"]
        success(f"Orders entity created: {state['orders_entity_id']}")
    else:
        error("Failed to create Orders entity")
        return False

    separator()

    # ---- Customers entity ----
    substep("Create 'customers' entity")
    customers_entity = {
        "name": "customers",
        "display_name": "Customers",
        "description": "Customer master data with segmentation and region",
        "pipeline_id": state["customers_pipeline_id"],
        "column_annotations": {
            "id": {"role": "primary_key", "description": "Unique customer identifier"},
            "name": {"role": "attribute", "description": "Customer first name"},
            "email": {"role": "attribute", "description": "Customer email address"},
            "segment": {"role": "dimension", "description": "Customer segment for analysis"},
            "region": {"role": "dimension", "description": "Customer geographic region"},
        },
    }
    resp = api_post("/ontology/entity", customers_entity)
    if resp.status_code == 200:
        state["customers_entity_id"] = resp.json()["id"]
        success(f"Customers entity created: {state['customers_entity_id']}")
    else:
        error("Failed to create Customers entity")
        return False

    separator()

    substep("List all entities")
    api_get("/ontology/entity/list")

    substep("Get entity by name: 'orders'")
    api_get("/ontology/entity/by-name/orders")
    return True


def step_3_create_relationship():
    """Create a foreign-key relationship: orders.customer_id -> customers.id"""
    step(3, "Create Relationship (orders -> customers)")

    info("Relationships define JOIN semantics for cross-entity queries.")

    rel_body = {
        "name": "orders_to_customers",
        "from_entity": "orders",
        "to_entity": "customers",
        "from_column": "customer_id",
        "to_column": "id",
        "relationship_type": "many_to_one",
        "description": "Each order belongs to one customer (orders.customer_id -> customers.id)",
    }
    resp = api_post("/ontology/relationship", rel_body)
    if resp.status_code == 200:
        state["relationship_id"] = resp.json()["id"]
        success(f"Relationship created: {state['relationship_id']}")
    else:
        error("Failed to create relationship")
        return False

    separator()

    substep("List all relationships")
    api_get("/ontology/relationship/list")
    return True


def step_4_create_metrics():
    """Create business metrics: revenue (SUM), average order value, order count."""
    step(4, "Create Business Metrics")

    info("Metrics are named SQL aggregations scoped to an entity.")

    metrics = [
        {
            "name": "revenue",
            "display_name": "Total Revenue",
            "description": "Sum of all order totals in USD",
            "entity_name": "orders",
            "expression": "SUM(orders.total)",
            "format_type": "currency",
        },
        {
            "name": "order_count",
            "display_name": "Order Count",
            "description": "Total number of orders",
            "entity_name": "orders",
            "expression": "COUNT(*)",
            "format_type": "number",
        },
        {
            "name": "avg_order_value",
            "display_name": "Average Order Value",
            "description": "Mean order total across all orders",
            "entity_name": "orders",
            "expression": "AVG(orders.total)",
            "format_type": "currency",
        },
        {
            "name": "total_quantity",
            "display_name": "Total Quantity",
            "description": "Sum of all item quantities ordered",
            "entity_name": "orders",
            "expression": "SUM(orders.quantity)",
            "format_type": "number",
        },
    ]

    for m in metrics:
        substep(f"Create metric: {m['name']}")
        resp = api_post("/ontology/metric", m)
        if resp.status_code == 200:
            state["metric_ids"].append(resp.json()["id"])
            success(f"Metric '{m['name']}' created: {resp.json()['id']}")
        else:
            warn(f"Metric '{m['name']}' creation returned {resp.status_code}")

    separator()

    substep("List all metrics")
    api_get("/ontology/metric/list")
    return True


def step_5_create_dimensions():
    """Create dimensions: customer_segment, customer_region, order_month."""
    step(5, "Create Dimensions")

    info("Dimensions are named GROUP BY expressions for semantic queries.")

    dimensions = [
        {
            "name": "customer_segment",
            "display_name": "Customer Segment",
            "description": "Group by customer segment",
            "entity_name": "customers",
            "expression": "customers.segment",
            "dimension_type": "direct",
        },
        {
            "name": "customer_region",
            "display_name": "Customer Region",
            "description": "Group by customer geographic region",
            "entity_name": "customers",
            "expression": "customers.region",
            "dimension_type": "direct",
        },
        {
            "name": "order_month",
            "display_name": "Order Month",
            "description": "Monthly grouping of order creation dates",
            "entity_name": "orders",
            "expression": "DATE_TRUNC('month', orders.created_at)",
            "dimension_type": "derived",
        },
        {
            "name": "order_status",
            "display_name": "Order Status",
            "description": "Group by order fulfilment status",
            "entity_name": "orders",
            "expression": "orders.status",
            "dimension_type": "direct",
        },
    ]

    for d in dimensions:
        substep(f"Create dimension: {d['name']}")
        resp = api_post("/ontology/dimension", d)
        if resp.status_code == 200:
            state["dimension_ids"].append(resp.json()["id"])
            success(f"Dimension '{d['name']}' created: {resp.json()['id']}")
        else:
            warn(f"Dimension '{d['name']}' creation returned {resp.status_code}")

    separator()

    substep("List all dimensions")
    api_get("/ontology/dimension/list")
    return True


def step_6_ontology_snapshot():
    """Show the full ontology snapshot -- entities, relationships, metrics, dimensions."""
    step(6, "Full Ontology Snapshot")

    info("The ontology snapshot returns every active element in one call.")
    info("This is what an AI agent reads to understand the data model.")

    resp = api_get("/ontology")
    if resp.status_code == 200:
        data = resp.json()
        print()
        print(f"  {MAGENTA}{BOLD}Ontology Summary:{RESET}")
        print(f"    Entities:      {len(data.get('entities', []))}")
        print(f"    Relationships: {len(data.get('relationships', []))}")
        print(f"    Metrics:       {len(data.get('metrics', []))}")
        print(f"    Dimensions:    {len(data.get('dimensions', []))}")
        success("Ontology snapshot retrieved successfully")
    return True


def step_7_semantic_query():
    """Try a structured semantic query: revenue by customer_segment."""
    step(7, "Structured Semantic Query")

    info("Semantic queries use metric/dimension names, not raw SQL.")
    info("The engine resolves them to SQL, builds JOINs, and executes.")
    warn("Execution requires pipeline runs with output files on S3.")
    warn("We will show the request/response shape; execution may fail without data.")

    query_body = {
        "metrics": ["revenue", "order_count"],
        "dimensions": ["customer_segment"],
        "order_by": ["revenue DESC"],
        "limit": 10,
    }

    substep("Query: revenue & order_count by customer_segment")
    resp = api_post("/ontology/query", query_body)
    if resp.status_code == 200:
        data = resp.json()
        if "generated_sql" in data:
            print()
            print(f"  {MAGENTA}{BOLD}Generated SQL:{RESET}")
            for line in data["generated_sql"].splitlines():
                print(f"    {CYAN}{line}{RESET}")
            success("Semantic query resolved to SQL successfully")
        else:
            success("Semantic query returned a result")
    elif resp.status_code == 400:
        data = resp.json()
        detail = data.get("detail", "")
        if "output_key" in str(detail).lower() or "parquet" in str(detail).lower() or "pipeline" in str(detail).lower():
            info("Expected: query execution requires pipeline output files on S3.")
            info("The ontology resolution (metric -> SQL expression) still works.")
            info("In a production setup with S3 configured, this would return actual data.")
        else:
            warn(f"Query returned 400: {detail}")
    else:
        warn(f"Query returned {resp.status_code} (may need pipeline data on S3)")

    separator()

    substep("Query: total_quantity by order_month")
    query_body_2 = {
        "metrics": ["total_quantity"],
        "dimensions": ["order_month"],
        "limit": 12,
    }
    resp = api_post("/ontology/query", query_body_2)

    separator()

    substep("Query: avg_order_value by customer_region")
    query_body_3 = {
        "metrics": ["avg_order_value"],
        "dimensions": ["customer_region"],
        "limit": 20,
    }
    resp = api_post("/ontology/query", query_body_3)

    return True


def step_8_proposal_workflow():
    """Demonstrate the ontology proposal workflow (propose -> list -> review)."""
    step(8, "Ontology Proposal Workflow")

    info("The /propose endpoint analyzes a pipeline and generates proposals")
    info("for entities, relationships, metrics, and dimensions.")
    info("In dev mode (REQUIRE_AUTH=false), proposals auto-approve.")

    # Propose for the orders pipeline
    substep(f"Propose ontology for Orders pipeline ({state['orders_pipeline_id']})")
    info("Note: proposals may conflict with manually created entities above.")
    info("Heuristic engine uses pipeline metadata (if available) to suggest elements.")

    propose_body = {
        "pipeline_id": state["orders_pipeline_id"],
        "include_relationships": True,
        "include_metrics": True,
    }
    resp = api_post("/ontology/propose", propose_body)

    if resp.status_code == 200:
        data = resp.json()
        count = data.get("count", 0)
        proposals = data.get("proposals", [])
        success(f"Received {count} proposal(s)")

        if proposals:
            for p in proposals:
                pid = p.get("id", "?")
                ptype = p.get("proposal_type", "?")
                pstatus = p.get("status", "?")
                print(f"    {DIM}[{pid}]{RESET} type={CYAN}{ptype}{RESET}  status={GREEN if pstatus == 'approved' else YELLOW}{pstatus}{RESET}")
                state["proposal_ids"].append(pid)
    elif resp.status_code == 400:
        detail = resp.json().get("detail", "")
        if "already exists" in detail.lower():
            info("Entity already exists from manual creation above -- expected.")
        else:
            warn(f"Propose returned 400: {detail}")
    else:
        warn(f"Propose returned {resp.status_code}")

    separator()

    substep("List all proposals")
    api_get("/ontology/proposal/list")

    separator()

    substep("Demonstrate proposal review (approve/reject)")
    info("In dev mode, proposals are auto-approved, so review is optional.")
    info("With REQUIRE_AUTH=true, proposals stay 'pending' until reviewed:")
    print(f"    {DIM}POST /ontology/proposal/{{id}}/review")
    print(f'    Body: {{"action": "approve"}}  or  {{"action": "reject", "notes": "reason"}}{RESET}')

    if state["proposal_ids"]:
        last_id = state["proposal_ids"][-1]
        substep(f"Get proposal detail: {last_id}")
        api_get(f"/ontology/proposal/{last_id}")

    return True


def step_9_capabilities():
    """Show that the capabilities endpoint includes ontology documentation."""
    step(9, "Capabilities Endpoint (Self-Describing API)")

    info("The /capabilities endpoint describes the full API for AI agents.")
    info("It includes the ontology section with all endpoints and workflows.")

    resp = api_get("/capabilities")
    if resp.status_code == 200:
        data = resp.json()
        ontology_section = data.get("ontology", {})
        if ontology_section:
            print()
            print(f"  {MAGENTA}{BOLD}Ontology section from /capabilities:{RESET}")
            formatted = json.dumps(ontology_section, indent=2)
            for line in formatted.splitlines():
                print(f"    {line}")
            success("Ontology is fully described in capabilities")
        else:
            warn("No ontology section found in capabilities")
    return True


def step_10_cleanup():
    """Optionally clean up created resources."""
    step(10, "Cleanup (optional)")

    info("Cleaning up demo resources to leave the server in a clean state.")

    # Delete metrics
    for mid in state["metric_ids"]:
        substep(f"Delete metric {mid}")
        api_delete(f"/ontology/metric/{mid}")

    # Delete dimensions
    for did in state["dimension_ids"]:
        substep(f"Delete dimension {did}")
        api_delete(f"/ontology/dimension/{did}")

    # Delete relationship
    if state["relationship_id"]:
        substep(f"Delete relationship {state['relationship_id']}")
        api_delete(f"/ontology/relationship/{state['relationship_id']}")

    # Delete entities
    if state["orders_entity_id"]:
        substep(f"Delete entity {state['orders_entity_id']}")
        api_delete(f"/ontology/entity/{state['orders_entity_id']}")
    if state["customers_entity_id"]:
        substep(f"Delete entity {state['customers_entity_id']}")
        api_delete(f"/ontology/entity/{state['customers_entity_id']}")

    # Delete pipelines
    if state["orders_pipeline_id"]:
        substep(f"Delete pipeline {state['orders_pipeline_id']}")
        api_delete(f"/pipeline/{state['orders_pipeline_id']}")
    if state["customers_pipeline_id"]:
        substep(f"Delete pipeline {state['customers_pipeline_id']}")
        api_delete(f"/pipeline/{state['customers_pipeline_id']}")

    separator()
    substep("Final ontology snapshot (should be empty)")
    api_get("/ontology")
    success("Cleanup complete")
    return True


# ===================================================================
# MAIN
# ===================================================================

def main():
    banner()

    # Health check
    if not check_health():
        sys.exit(1)

    steps = [
        step_1_create_pipelines,
        step_2_create_entities,
        step_3_create_relationship,
        step_4_create_metrics,
        step_5_create_dimensions,
        step_6_ontology_snapshot,
        step_7_semantic_query,
        step_8_proposal_workflow,
        step_9_capabilities,
    ]

    for fn in steps:
        try:
            ok = fn()
            if ok is False:
                error(f"Step failed: {fn.__name__}")
                warn("Continuing with remaining steps...")
        except requests.ConnectionError:
            error("Lost connection to server. Is it still running?")
            sys.exit(1)
        except Exception as e:
            error(f"Unexpected error in {fn.__name__}: {e}")
            warn("Continuing with remaining steps...")

    # Ask about cleanup
    separator()
    print()
    print(f"  {BOLD}Demo complete!{RESET}")
    print()
    try:
        answer = input(f"  {YELLOW}Clean up demo resources? [y/N]: {RESET}").strip().lower()
        if answer in ("y", "yes"):
            step_10_cleanup()
        else:
            info("Skipping cleanup. Resources remain on the server.")
            info(f"Orders pipeline:   {state['orders_pipeline_id']}")
            info(f"Customers pipeline: {state['customers_pipeline_id']}")
    except (KeyboardInterrupt, EOFError):
        print()
        info("Skipping cleanup.")

    print()
    print(f"  {BG_BLUE}{WHITE}{BOLD} Done. {RESET}  Thanks for trying the Relay ontology layer!")
    print()


if __name__ == "__main__":
    main()
