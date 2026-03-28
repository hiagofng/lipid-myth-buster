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

## Limitations & Assumptions

- **Waist circumference unavailable**: BMI >= 30 is used as a proxy for waist circumference when computing the Metabolic Syndrome Score. The real NCEP ATP III criteria use waist circumference (>102 cm for men, >88 cm for women), which is not present in this dataset.
- **LDL particle size is estimated, not measured**: The TG/HDL ratio is used as a population-level proxy for LDL particle size (Pattern A vs Pattern B), based on published research (Maruyama et al. 2003, Boizel et al. 2000). A direct NMR lipoprotein panel would be more accurate but is not available in this dataset.
- **Missing data**: ~10.7% of subjects (180 out of 1,688) have missing LDL cholesterol values, and 4 subjects have missing CAC scores. These are labeled as "Unknown" in derived category columns rather than dropped, to preserve sample size.
- **Single ethnic population**: The dataset consists of Korean asymptomatic health screening participants. Findings may not generalize to other ethnicities or symptomatic populations.
- **Cross-sectional data**: This is a single-timepoint snapshot, not a longitudinal study. We can show associations (e.g., metabolic health correlates with CAC) but cannot prove causation.
- **Statin use unknown — LDL values may be artificially suppressed**: The dataset contains no information on lipid-lowering medication use. A meaningful proportion of subjects, particularly those with metabolic risk factors, may be on statins or other LDL-lowering drugs. Statins can reduce LDL by 30–50%, causing individuals with historically high LDL to appear in the "Optimal" or "Near optimal" LDL categories despite having significant prior cardiovascular risk. Critically, statins do not reverse existing arterial calcification — they may even increase CAC scores by stabilizing soft plaques into calcified ones. This creates a structural confound: the "Optimal LDL + Metabolically Unhealthy" group likely contains a disproportionate share of medicated, higher-risk individuals, inflating their average CAC and distorting the apparent LDL-CAC relationship. Any analysis relying on LDL as a predictor should be interpreted with this limitation in mind. The TG/HDL ratio and metabolic syndrome score are substantially less affected by statin use and are therefore more reliable signals in this dataset.

## Architecture Diagram
(Add draw.io PNG here later)

## How to Run
```bash
make up      # docker-compose up
bruin run    # or scheduled pipeline