from src.metadata import MetadataGenerator
import pandas as pd

# Load sample
df = pd.read_parquet('test_chunk.parquet')
metadata_gen = MetadataGenerator()

# Generate metadata
metadata = metadata_gen.generate_metadata(
    df,
    '10M Synthetic Customer Records',
    {'type': 'synthetic'}
)

# Save it
metadata_gen.save_metadata(metadata, 'pipe-4752f81e')
print('Metadata generated and saved!')
print(f'Columns needing review: {metadata["columns_needing_review"]}')
