# Validate your pipeline (checks YAML, column defs, dependencies)
bruin validate bruin/pipeline

# Run just the ingestion asset
bruin run bruin/pipeline/assets/ingestion/cac_health_screening.py

# Run the full pipeline
bruin run bruin/pipeline

# Quick check with DuckDB CLI
duckdb lipid_myth_buster.duckdb "SELECT count(*) FROM cac_health_screening"