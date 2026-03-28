# Staging Instructions for LipidMythBuster

## The Big Picture

Staging sits between raw ingestion and final reports. It's where you clean, enrich, and prepare data for analysis.

```
  INGESTION (raw data)              STAGING (clean data)                REPORTS (analytics)
  ┌─────────────────────┐           ┌──────────────────────────┐       ┌──────────────────┐
  │ ingestion.           │           │ staging.                  │       │ reports.          │
  │ cac_health_screening │ ──SQL──> │ stg_cac_health_screening  │ ───> │ (next step)       │
  │                      │           │                           │       │                   │
  │ Raw columns as-is    │           │ + null handling           │       │ Aggregated tiles  │
  │ No derived fields    │           │ + derived risk categories │       │ for dashboard     │
  │ No quality checks    │           │ + smoking_status merged   │       │                   │
  └─────────────────────┘           │ + quality checks          │       └──────────────────┘
                                    └──────────────────────────┘
```

**Why not transform in the ingestion step?**
Separation of concerns. If your transformation logic has a bug, you don't need to re-download the data. The raw layer is your safety net — always a clean copy of the source.

---

## What the Staging Asset Does

Your staging SQL will read from `ingestion.cac_health_screening` and produce a cleaner, enriched table. Specifically:

### A) Handle nulls
Some biomarker columns may have missing values. You need to decide: keep them as NULL, or exclude those rows? For clinical data, keeping NULLs is usually better — dropping rows loses information.

### B) Create derived columns
This is where LipidMythBuster gets interesting. You'll create categories that help the dashboard tell the story:

**CAC Risk Category** — Based on the Coronary Artery Calcium Score:
```
CACS = 0          → "No calcification"
CACS 1-99         → "Mild"
CACS 100-399      → "Moderate"
CACS >= 400       → "Severe"
```
These are standard clinical cutoffs used in cardiology.

**LDL Category** — Standard medical thresholds:
```
LDL < 100         → "Optimal"
LDL 100-129       → "Near optimal"
LDL 130-159       → "Borderline high"
LDL 160-189       → "High"
LDL >= 190        → "Very high"
```

**Metabolic Risk Flags** — This is the core of your thesis ("metabolic health matters more than LDL"):
```
TG >= 150              → high_triglycerides (flag)
Glucose >= 100         → high_glucose (flag)
HDL < 40 (M) / <50 (F) → low_hdl (flag)
BMI >= 30              → obese (flag)
HbA1c >= 5.7           → prediabetic_or_diabetic (flag)
```

**Smoking Status** — Merge the two binary columns into one:
```
current_smoker = 1  → "Current"
ex_smoker = 1       → "Former"
both = 0            → "Never"
```

### C) Add quality checks
Bruin lets you define checks in the SQL asset header. These run after the SQL and fail the pipeline if data looks wrong. Example: `no_figshare` should never be null, `cacs` should be >= 0.

---

## Step-by-Step Plan

### Step 1: Create the SQL file

Create: `bruin/pipeline/assets/staging/stg_cac_health_screening.sql`

A Bruin SQL asset has two parts:
1. A `/* @bruin ... @bruin */` comment block at the top (like the Python `"""@bruin"""` header)
2. A `SELECT` statement that IS the transformation

### Step 2: Write the Bruin header

```sql
/* @bruin
name: staging.stg_cac_health_screening
type: duckdb.sql
materialization:
  type: table
  strategy: create+replace

depends:
  - ingestion.cac_health_screening

columns:
  - name: no_figshare
    type: integer
    description: "Subject ID"
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: cacs
    type: float
    description: "Coronary Artery Calcium Score"
    checks:
      - name: not_null
      - name: positive
  - name: cac_risk_category
    type: string
    description: "CAC risk level: No calcification / Mild / Moderate / Severe"
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - "No calcification"
          - "Mild"
          - "Moderate"
          - "Severe"
          - "Unknown"
  - name: ldl_category
    type: string
    description: "LDL classification per ATP III guidelines"
    checks:
      - name: not_null
      - name: accepted_values
        values:
          - "Optimal"
          - "Near optimal"
          - "Border line high"
          - "High"
          - "Very high"
          - "Unknown"
  - name: smoking_status
    type: string
    description: "Derived from ex_smoker + current_smoker flags"
    checks:
      - name: accepted_values
        value:
          - "Current"
          - "Former"
          - "Never"
@bruin */
```

**Key things to notice:**

- `type: duckdb.sql` — tells Bruin this is a SQL asset running on DuckDB
- `depends: - ingestion.cac_health_screening` — Bruin will run ingestion FIRST, then staging. This is how the DAG is built.
- `strategy: create+replace` — same as ingestion, since we're rebuilding from the full raw table each time
- `checks:` — quality gates. If `cacs` has a negative value, the pipeline fails and alerts you.

### Step 3: Write the SELECT statement

After the `@bruin */` closing comment, write a single SELECT. This is the entire transformation.

The SQL structure will look like:

```sql
SELECT
    -- 1. Pass through the original columns you want to keep
    no_figshare,
    age_yr,
    sex,
    sbp,
    dbp,
    weight,
    bmi,
    glucose,
    chol,
    creatinine,
    idms_mdrd_gfr,
    tg,
    hdl_chol,
    ldl_chol,
    hs_crp,
    hb_a1c,
    hypertension,
    diabetes,
    cacs,

    -- 2. Derive: CAC risk category
    CASE
        WHEN cacs = 0              THEN 'No calcification'
        WHEN cacs BETWEEN 1 AND 99 THEN 'Mild'
        WHEN cacs BETWEEN 100 AND 399 THEN 'Moderate'
        WHEN cacs >= 400           THEN 'Severe'
        ELSE "Unknown"
    END AS cac_risk_category,

    -- 3. Derive: LDL cholesterol category (ATP III guidelines)
    CASE
        WHEN ldl_chol < 100  THEN 'Optimal'
        WHEN ldl_chol < 130  THEN 'Near optimal'
        WHEN ldl_chol < 160  THEN 'Borderline high'
        WHEN ldl_chol < 190  THEN 'High'
        WHEN ldl_chol >= 190 THEN 'Very high'
    END AS ldl_category,

    -- 4. Derive: metabolic risk flags
    CASE WHEN tg >= 150 THEN 1 ELSE 0 END AS high_triglycerides,
    CASE WHEN glucose >= 100 THEN 1 ELSE 0 END AS high_glucose,
    CASE
        WHEN sex = 'M' AND hdl_chol < 40 THEN 1
        WHEN sex = 'F' AND hdl_chol < 50 THEN 1
        ELSE 0
    END AS low_hdl,
    CASE WHEN bmi >= 30 THEN 1 ELSE 0 END AS obese,
    CASE WHEN hb_a1c >= 5.7 THEN 1 ELSE 0 END AS prediabetic_or_diabetic,

    -- 5. Derive: smoking status (merge two binary columns)
    CASE
        WHEN current_smoker = 1 THEN 'Current'
        WHEN ex_smoker = 1      THEN 'Former'
        ELSE 'Never'
    END AS smoking_status,

    -- 6. Keep lineage timestamp from ingestion
    extracted_at

FROM ingestion.cac_health_screening
WHERE no_figshare IS NOT NULL
```

**What's happening here:**
- Lines 1: pass-through columns — you keep the raw values as-is
- Lines 2-3: clinical categories — turn continuous numbers into meaningful buckets
- Lines 4: metabolic flags — binary 0/1 flags based on medical thresholds. These are what your dashboard will use to argue "metabolic health matters more than LDL"
- Line 5: smoking status — collapses two columns into one cleaner column
- Line 6: lineage — carry forward when the data was ingested
- WHERE: basic filter to drop any rows without an ID (shouldn't happen, but safety net)

### Step 4: Validate and Run

```bash
# Validate — should now show 2 assets (ingestion + staging)
bruin validate bruin/pipeline

# Run just the staging asset
bruin run bruin/pipeline/assets/staging/stg_cac_health_screening.sql

# Or run everything (ingestion will be skipped if already done, staging runs after)
bruin run bruin/pipeline
```

### Step 5: Verify in your notebook

```python
conn.sql("SELECT * FROM staging.stg_cac_health_screening LIMIT 10")
```

Check that:
- `cac_risk_category` has values like "No calcification", "Mild", etc.
- `ldl_category` has values like "Optimal", "Borderline high", etc.
- `smoking_status` shows "Current", "Former", or "Never"
- The metabolic flags are 0 or 1

---

## How This Connects to Your Thesis

The whole point of LipidMythBuster is to show:

```
Common belief:  High LDL  ──────────────>  Heart disease (CAC)
Your thesis:    Metabolic health  ────────>  Heart disease (CAC)
                (TG, glucose, HDL, HbA1c)     matters MORE than LDL alone
```

The staging layer creates the columns you need to prove this on the dashboard:
- Compare `cac_risk_category` distribution across `ldl_category` groups
- Compare `cac_risk_category` distribution across metabolic flag combinations
- If people with "Very high" LDL don't have worse CAC than people with "Optimal" LDL BUT people with high triglycerides DO have worse CAC... that's your story.

---

## Summary Checklist

```
[ ] 1. Create stg_cac_health_screening.sql in staging/
[ ] 2. Write the @bruin header (type, depends, columns, checks)
[ ] 3. Write the SELECT with derived columns
[ ] 4. bruin validate --> should show 2 assets, no errors
[ ] 5. bruin run --> staging table created
[ ] 6. Verify in notebook (check derived columns look right)
```

---

## Common Pitfalls

| Pitfall | What happens | Fix |
|---------|-------------|-----|
| Missing `depends:` | Bruin runs staging before ingestion, fails because table doesn't exist | Always declare dependencies |
| Using `type: sql` instead of `type: duckdb.sql` | Bruin doesn't know which engine to use | Use `duckdb.sql` for DuckDB assets |
| CASE without ELSE | Rows that don't match any WHEN get NULL | Add ELSE for default values |
| Wrong column name in SELECT | DuckDB error: "column X not found" | Double-check against your ingestion columns |
| Quality check on nullable column | Check fails on NULLs you intended to keep | Only add `not_null` checks on columns that truly should never be null |
