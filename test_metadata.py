from src.metadata import MetadataGenerator
import pandas as pd
import json

df = pd.read_parquet('test_chunk.parquet')
metadata_gen = MetadataGenerator()

metadata = metadata_gen.generate_metadata(
    df,
    '10M Synthetic Customer Records',
    {'type': 'synthetic'}
)

print('METADATA GENERATED:')
print('='*60)
print(f'Total columns: {metadata["column_count"]}')
print(f'Columns needing review: {metadata["columns_needing_review"]}')
print()
print('COLUMN ANALYSIS:')
for col in metadata['columns']:
    print(f'\n{col["name"]}:')
    print(f'  Type: {col["type"]}')
    print(f'  Semantic type: {col["semantic_type"]}')
    print(f'  Null %: {col["null_percentage"]}%')
    print(f'  Auto description: {col["auto_description"]}')
    print(f'  Sample values: {col["sample_values"][:3]}')
    print(f'  Needs review: {col["needs_review"]}')
    print(f'  Human verified: {col.get("human_verified", False)}')
