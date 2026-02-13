#!/usr/bin/env python3
"""
One-time migration: pipelines.json + metadata/*.json -> SQLite

Usage:
    python -m scripts.migrate_json_to_sqlite
"""

import json
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database import init_db
from src.storage import Storage


def main():
    pipelines_file = project_root / "pipelines.json"
    metadata_dir = project_root / "metadata"

    if not pipelines_file.exists():
        print("No pipelines.json found -- nothing to migrate.")
        return

    # Initialize database
    init_db()
    storage = Storage()

    # Load pipelines
    with open(pipelines_file) as f:
        data = json.load(f)

    pipelines = data.get("pipelines", [])
    print(f"Found {len(pipelines)} pipelines to migrate.")

    for p in pipelines:
        pipeline_id = p["id"]
        try:
            # Save pipeline (without runs, they'll be added separately)
            runs = p.pop("runs", [])
            storage.save_pipeline(p)

            # Add each run
            for run in runs:
                storage.add_run(pipeline_id, run)
                # Update with full run data
                storage.update_run(pipeline_id, run["run_id"], run)

            print(f"  Migrated pipeline: {pipeline_id} ({p['name']}) with {len(runs)} runs")
        except Exception as e:
            print(f"  FAILED pipeline {pipeline_id}: {e}")

    # Migrate metadata files
    if metadata_dir.exists():
        for meta_file in metadata_dir.glob("*_metadata.json"):
            pipeline_id = meta_file.stem.replace("_metadata", "")
            try:
                with open(meta_file) as f:
                    metadata = json.load(f)
                storage.save_metadata(pipeline_id, metadata)
                print(f"  Migrated metadata for: {pipeline_id}")
            except Exception as e:
                print(f"  FAILED metadata {pipeline_id}: {e}")

        # Migrate knowledge base
        kb_file = metadata_dir / "knowledge_base.json"
        if kb_file.exists():
            with open(kb_file) as f:
                kb = json.load(f)
            for col_key, info in kb.get("verified_columns", {}).items():
                storage.save_column_knowledge(
                    col_key,
                    info["description"],
                    info.get("business_meaning"),
                    info.get("verified_by", "user"),
                )
                print(f"  Migrated column knowledge: {col_key}")

    print("\nMigration complete!")


if __name__ == "__main__":
    main()
