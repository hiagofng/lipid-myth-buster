# LipidMythBuster 🫀💥

Data Engineering Zoomcamp 2026 Final Project  
**Questioning the LDL Myth**: Using real blood exam + Coronary Artery Calcium (CAC) data to show metabolic health (glucose, triglycerides, etc.) often matters more than high LDL alone for heart disease risk.

## Tech Stack
- **Ingestion & Orchestration**: Bruin (ingestion assets, SQL/Python transforms, schedules)
- **Transformations**: Bruin + dbt (hybrid for max learning)
- **Warehouse**: Postgres (local) / BigQuery (cloud)
- **Dashboards**: Metabase / Looker Studio (visualizing LDL vs. metabolic vs. CAC)
- **Other**: Docker, Terraform (optional infra)

Dataset: Korean Asymptomatic Health Screening CAC (~1,688 rows, public on Figshare)  
Link: https://figshare.com/articles/dataset/Clinical_characteristics_CACS_GWAS_n_1688/7853588

## Architecture Diagram
(Add draw.io PNG here later)

## How to Run
```bash
make up      # docker-compose up
bruin run    # or scheduled pipeline