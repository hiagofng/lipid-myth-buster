"""@bruin
name: ingestion.cac_health_screening
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: create+replace

columns:
  - name: no_figshare
    type: integer
    description: "Unique subject identifier from the Figshare dataset"
    primary_key: true
  - name: test1_replication2
    type: integer
    description: "Cohort flag: 1 = discovery set, 2 = replication set (GWAS study design)"
  - name: sbp
    type: float
    description: "Systolic blood pressure (mmHg)"
  - name: dbp
    type: float
    description: "Diastolic blood pressure (mmHg)"
  - name: weight
    type: float
    description: "Body weight (kg)"
  - name: bmi
    type: float
    description: "Body Mass Index (kg/m^2)"
  - name: glucose
    type: float
    description: "Fasting blood glucose (mg/dL)"
  - name: chol
    type: float
    description: "Total cholesterol (mg/dL)"
  - name: creatinine
    type: float
    description: "Serum creatinine (mg/dL)"
  - name: idms_mdrd_gfr
    type: float
    description: "Estimated glomerular filtration rate via IDMS-traceable MDRD formula (mL/min/1.73m^2)"
  - name: tg
    type: float
    description: "Triglycerides (mg/dL) — key metabolic health marker"
  - name: hdl_chol
    type: float
    description: "HDL cholesterol (mg/dL) — 'good' cholesterol"
  - name: ldl_chol
    type: float
    description: "LDL cholesterol (mg/dL) — the 'myth' variable this project investigates"
  - name: hs_crp
    type: float
    description: "High-sensitivity C-reactive protein (mg/L) — inflammation marker"
  - name: hb_a1c
    type: float
    description: "Hemoglobin A1c (%) — 3-month average blood sugar indicator"
  - name: hypertension
    type: integer
    description: "Hypertension diagnosis flag (0 = no, 1 = yes)"
  - name: diabetes
    type: integer
    description: "Diabetes diagnosis flag (0 = no, 1 = yes)"
  - name: ex_smoker
    type: integer
    description: "Former smoker flag (0 = no, 1 = yes)"
  - name: current_smoker
    type: integer
    description: "Current smoker flag (0 = no, 1 = yes)"
  - name: age_yr
    type: integer
    description: "Age in years at time of screening"
  - name: sex
    type: string
    description: "Biological sex (M/F)"
  - name: cacs
    type: float
    description: "Coronary Artery Calcium Score — the outcome variable measuring arterial calcification"
  - name: extracted_at
    type: timestamp
    description: "Timestamp of when this row was ingested by the pipeline"

@bruin"""

# ---------------------------------------------------------
# Ingestion: Korean Asymptomatic Health Screening CAC Data
# ---------------------------------------------------------
# Source: Figshare (CC BY 4.0) — 1,688 subjects with blood
# biomarkers and Coronary Artery Calcium Scores (CACS).
# URL: https://figshare.com/articles/dataset/Clinical_characteristics_CACS_GWAS_n_1688/7853588
#
# NOTE: Figshare blocks automated downloads (WAF bot protection),
# so the .xlsx file must be downloaded manually to data/raw/.
# This asset reads from that local copy, cleans column names
# to be DuckDB-friendly, and returns a DataFrame for Bruin
# to load into the warehouse via create+replace strategy.
# ---------------------------------------------------------

import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# Local path to the manually downloaded Excel file.
# The file lives in data/raw/ at the project root.
# __file__ resolves to: bruin/pipeline/assets/ingestion/cac_health_screening.py
# We navigate up to the project root (4 levels) then into data/raw/.
# File should be named "clinical_cac_1688.xlsx" to match SOURCE_PATH below.
PROJECT_ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = PROJECT_ROOT / "data" / "raw" / "clinical_cac_1688.xlsx"


def materialize():
    """Read the CAC dataset from local Excel file and return a clean DataFrame.

    Bruin calls this function automatically. The returned DataFrame is
    loaded into DuckDB as the `cac_health_screening` table.
    """

    # 1. Read the locally stored Excel file
    if not SOURCE_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found at {SOURCE_PATH}. "
            "Download it manually from Figshare and save it to data/raw/clinical_cac_1688.xlsx"
        )

    df = pd.read_excel(SOURCE_PATH, engine="openpyxl")

    # 2. Normalize column names: lowercase, replace dots/hyphens/spaces with underscores,
    #    and strip trailing punctuation. This avoids DuckDB quoting issues.
    #    e.g. "HDL.CHOL." -> "hdl_chol", "Ex-smoker" -> "ex_smoker"
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[.\-\s]+", "_", regex=True)  # dots, hyphens, spaces -> underscore
        .str.strip("_")                               # remove trailing underscores
    )

    # 3. Add lineage column so we know when this data was ingested
    df["extracted_at"] = datetime.now(timezone.utc)

    return df
