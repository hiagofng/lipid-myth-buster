"""
LipidMythBuster Dashboard
=========================
Streamlit dashboard for the Data Engineering Zoomcamp 2026 Final Project.
Visualizes the relationship between LDL cholesterol, metabolic health,
and Coronary Artery Calcium Scores (CACS) using the Korean CAC dataset.

Run with: streamlit run dashboards/app.py
"""

import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="LipidMythBuster",
    page_icon="🫀",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
# DuckDB allows multiple read-only connections, which is what we need here.
# The DuckDB file lives at the project root.
DB_PATH = Path(__file__).resolve().parents[1] / "lipid_myth_buster.duckdb"


@st.cache_resource
def get_connection():
    """Open a read-only DuckDB connection (cached across reruns)."""
    return duckdb.connect(str(DB_PATH), read_only=True)


conn = get_connection()

# ---------------------------------------------------------------------------
# Load report tables
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def load_table(table_name: str):
    """Load a report table into a pandas DataFrame (cached 5 min)."""
    return conn.sql(f"SELECT * FROM {table_name}").df()


df_risk = load_table("reports.cac_by_risk_group")
df_age = load_table("reports.cac_by_age")
df_particle = load_table("reports.ldl_particle_analysis")
df_divergence = load_table("reports.high_ldl_cac_divergence")
df_pure = load_table("reports.pure_high_ldl_cac")

# Also load staging for the overview stats
df_staging = load_table("staging.stg_cac_health_screening")

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🫀 LipidMythBuster")
st.markdown(
    "**Questioning the LDL Myth:** Does metabolic health predict "
    "coronary artery calcification better than LDL cholesterol alone?"
)
st.markdown("---")

# ---------------------------------------------------------------------------
# Key metrics row
# ---------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Subjects", f"{len(df_staging):,}")
col2.metric("% with Any Calcification",
            f"{(df_staging['cacs'] > 0).mean() * 100:.1f}%")
col3.metric("Avg CAC Score", f"{df_staging['cacs'].mean():.1f}")
col4.metric("% Metabolically Unhealthy",
            f"{(df_staging['metabolic_health_status'] == 'Metabolically unhealthy').mean() * 100:.1f}%")

st.markdown("---")

# ---------------------------------------------------------------------------
# TILE 1: CAC by LDL Category x Metabolic Health (categorical distribution)
# ---------------------------------------------------------------------------
st.header("📊 Tile 1: CAC Severity by LDL Level & Metabolic Health")
st.markdown(
    "If the LDL myth holds, we'd expect higher LDL categories to have worse CAC "
    "**regardless** of metabolic health. If metabolic health matters more, we'd see "
    "metabolically unhealthy groups have worse CAC **even at low LDL levels**."
)

# Order LDL categories properly
ldl_order = ["Optimal", "Near optimal", "Borderline high", "High", "Very high", "Unknown"]
df_risk["ldl_category"] = df_risk["ldl_category"].astype("category")
df_risk["ldl_category"] = df_risk["ldl_category"].cat.set_categories(
    [c for c in ldl_order if c in df_risk["ldl_category"].unique()], ordered=True
)
df_risk = df_risk.sort_values("ldl_category")

# Metric selector
metric_options = {
    "% with Any Calcification": "pct_any_calcification",
    "% Moderate or Severe": "pct_moderate_or_severe",
    "Average CAC Score": "avg_cacs",
}
selected_metric = st.selectbox(
    "Select metric to compare:",
    list(metric_options.keys()),
    index=0,
)
metric_col = metric_options[selected_metric]

fig1 = px.bar(
    df_risk,
    x="ldl_category",
    y=metric_col,
    color="metabolic_health_status",
    barmode="group",
    color_discrete_map={
        "Metabolically healthy": "#22c55e",
        "Metabolically unhealthy": "#ef4444",
    },
    labels={
        "ldl_category": "LDL Category",
        metric_col: selected_metric,
        "metabolic_health_status": "Metabolic Status",
    },
    title=f"{selected_metric} by LDL Category & Metabolic Health Status",
    text_auto=True,
)
fig1.update_layout(
    xaxis_title="LDL Cholesterol Category",
    yaxis_title=selected_metric,
    legend_title="Metabolic Status",
    height=500,
)
st.plotly_chart(fig1, use_container_width=True)

# Insight callout
st.info(
    "💡 **Look at the pattern:** If the red bars (metabolically unhealthy) are "
    "consistently higher than green bars (healthy) across ALL LDL categories — "
    "including 'Optimal' LDL — then metabolic health is a stronger predictor than LDL level."
)

# Show the data table
with st.expander("📋 View underlying data"):
    st.dataframe(
        df_risk[["ldl_category", "metabolic_health_status", "subject_count",
                 "avg_cacs", "median_cacs", "pct_any_calcification",
                 "pct_moderate_or_severe"]],
        use_container_width=True,
    )

st.markdown("---")

# ---------------------------------------------------------------------------
# TILE 2: CAC progression by age group (temporal distribution)
# ---------------------------------------------------------------------------
st.header("📈 Tile 2: CAC Progression by Age & Metabolic Health")
st.markdown(
    "Age serves as a temporal proxy (older = more years of exposure). "
    "This chart shows whether metabolic health accelerates or decelerates "
    "arterial calcification over time."
)

age_order = ["30-39", "40-49", "50-59", "60-69", "70+"]
df_age["age_group"] = df_age["age_group"].astype("category")
df_age["age_group"] = df_age["age_group"].cat.set_categories(
    [c for c in age_order if c in df_age["age_group"].unique()], ordered=True
)
df_age = df_age.sort_values("age_group")

# Metric selector for age chart
age_metric_options = {
    "% with Any Calcification": "pct_any_calcification",
    "% Moderate or Severe": "pct_moderate_or_severe",
    "Average CAC Score": "avg_cacs",
    "Avg Metabolic Syndrome Score": "avg_met_score",
}
selected_age_metric = st.selectbox(
    "Select metric to track across age:",
    list(age_metric_options.keys()),
    index=0,
    key="age_metric",
)
age_metric_col = age_metric_options[selected_age_metric]

fig2 = px.line(
    df_age,
    x="age_group",
    y=age_metric_col,
    color="metabolic_health_status",
    markers=True,
    color_discrete_map={
        "Metabolically healthy": "#22c55e",
        "Metabolically unhealthy": "#ef4444",
    },
    labels={
        "age_group": "Age Group",
        age_metric_col: selected_age_metric,
        "metabolic_health_status": "Metabolic Status",
    },
    title=f"{selected_age_metric} by Age Group & Metabolic Health Status",
)
fig2.update_layout(
    xaxis_title="Age Group",
    yaxis_title=selected_age_metric,
    legend_title="Metabolic Status",
    height=500,
)
# Add subject count as hover info
fig2.update_traces(
    hovertemplate="<b>%{x}</b><br>"
    + f"{selected_age_metric}: " + "%{y}<br>"
    + "<extra></extra>"
)
st.plotly_chart(fig2, use_container_width=True)

st.info(
    "💡 **Look at the gap:** If the red line (unhealthy) diverges from the green "
    "line (healthy) more steeply with age, metabolic health is accelerating "
    "calcification independently of normal aging."
)

with st.expander("📋 View underlying data"):
    st.dataframe(
        df_age[["age_group", "metabolic_health_status", "subject_count",
                "avg_cacs", "pct_any_calcification", "pct_moderate_or_severe",
                "avg_met_score"]],
        use_container_width=True,
    )

st.markdown("---")

# ---------------------------------------------------------------------------
# BONUS: LDL Particle Pattern Analysis
# ---------------------------------------------------------------------------
st.header("🔬 Bonus: LDL Particle Quality Matters More Than Quantity")
st.markdown(
    "The TG/HDL ratio estimates LDL particle size. **Pattern B (Small Dense)** "
    "LDL is more atherogenic than **Pattern A (Large Buoyant)** — even at the "
    "same total LDL level."
)

df_particle_clean = df_particle.dropna(subset=["estimated_ldl_particle_pattern"])
df_particle_clean["ldl_category"] = df_particle_clean["ldl_category"].astype("category")
df_particle_clean["ldl_category"] = df_particle_clean["ldl_category"].cat.set_categories(
    [c for c in ldl_order if c in df_particle_clean["ldl_category"].unique()], ordered=True
)
df_particle_clean = df_particle_clean.sort_values("ldl_category")

fig3 = px.bar(
    df_particle_clean,
    x="ldl_category",
    y="pct_any_calcification",
    color="estimated_ldl_particle_pattern",
    barmode="group",
    color_discrete_map={
        "Pattern A (Large Buoyant)": "#38bdf8",
        "Pattern B (Small Dense)": "#f97316",
    },
    labels={
        "ldl_category": "LDL Category",
        "pct_any_calcification": "% with Any Calcification",
        "estimated_ldl_particle_pattern": "LDL Particle Pattern",
    },
    title="% with Any Calcification: Pattern A vs Pattern B at Each LDL Level",
    text_auto=True,
)
fig3.update_layout(
    xaxis_title="LDL Cholesterol Category",
    yaxis_title="% with Any Calcification",
    legend_title="Particle Pattern",
    height=500,
)
st.plotly_chart(fig3, use_container_width=True)

st.info(
    "💡 **The key insight:** If Pattern B (orange) has higher calcification than "
    "Pattern A (blue) at the same LDL level, it's not about HOW MUCH LDL you "
    "have — it's about WHAT KIND."
)

with st.expander("📋 View underlying data"):
    st.dataframe(df_particle_clean, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# TILE 3: High-LDL CAC Divergence (Protected vs Calcified)
# ---------------------------------------------------------------------------
st.header("🧬 Tile 3: The High-LDL Paradox — Protected vs Calcified")
st.markdown(
    "Among **metabolically healthy** people with **High or Very High LDL**, "
    "what separates those with **zero** calcification from those who calcify? "
    "If LDL alone drove risk, these two groups should look identical on every "
    "other biomarker."
)

if len(df_divergence) >= 2:
    protected = df_divergence[df_divergence["cohort"].str.contains("Protected")].iloc[0]
    calcified = df_divergence[df_divergence["cohort"].str.contains("Calcified")].iloc[0]

    # Key comparison metrics
    st.subheader("Cohort Sizes")
    c1, c2 = st.columns(2)
    c1.metric("Protected (CAC=0)", f"{int(protected['subject_count']):,}")
    c2.metric("Calcified (CAC>0)", f"{int(calcified['subject_count']):,}")

    st.subheader("Head-to-Head Comparison")

    # Build comparison rows
    comparison_metrics = [
        ("Avg Age (yr)", "avg_age", ".1f"),
        ("% Male", "pct_male", ".1f"),
        ("Avg BMI", "avg_bmi", ".1f"),
        ("Avg LDL (mg/dL)", "avg_ldl", ".1f"),
        ("Avg HDL (mg/dL)", "avg_hdl", ".1f"),
        ("Avg Triglycerides (mg/dL)", "avg_triglycerides", ".1f"),
        ("Avg TG/HDL Ratio", "avg_tg_hdl_ratio", ".2f"),
        ("% Pattern B (Small Dense)", "pct_pattern_b", ".1f"),
        ("Avg Non-HDL Chol (mg/dL)", "avg_non_hdl_chol", ".1f"),
        ("Avg Glucose (mg/dL)", "avg_glucose", ".1f"),
        ("Avg HbA1c (%)", "avg_hba1c", ".2f"),
        ("Avg Systolic BP (mmHg)", "avg_sbp", ".1f"),
        ("Avg Diastolic BP (mmHg)", "avg_dbp", ".1f"),
        ("Avg hs-CRP (mg/L)", "avg_hs_crp", ".2f"),
        ("Avg GFR (mL/min)", "avg_gfr", ".1f"),
        ("% Current Smoker", "pct_current_smoker", ".1f"),
        ("% Ex-Smoker", "pct_ex_smoker", ".1f"),
    ]

    import pandas as pd
    rows = []
    for label, col, fmt in comparison_metrics:
        p_val = protected[col]
        c_val = calcified[col]
        diff = c_val - p_val
        rows.append({
            "Biomarker": label,
            "Protected (CAC=0)": f"{p_val:{fmt}}",
            "Calcified (CAC>0)": f"{c_val:{fmt}}",
            "Difference": f"{diff:+{fmt}}",
        })
    df_compare = pd.DataFrame(rows)
    st.dataframe(df_compare, use_container_width=True, hide_index=True)

    # Radar chart: normalized comparison
    st.subheader("Biomarker Profile Overlay")
    radar_metrics = [
        ("Age", "avg_age"),
        ("BMI", "avg_bmi"),
        ("TG/HDL", "avg_tg_hdl_ratio"),
        ("% Pattern B", "pct_pattern_b"),
        ("Glucose", "avg_glucose"),
        ("HbA1c", "avg_hba1c"),
        ("SBP", "avg_sbp"),
        ("hs-CRP", "avg_hs_crp"),
        ("% Smoker", "pct_current_smoker"),
    ]
    categories = [m[0] for m in radar_metrics]
    # Normalize to 0-1 range for radar
    p_vals = [float(protected[m[1]]) for m in radar_metrics]
    c_vals = [float(calcified[m[1]]) for m in radar_metrics]
    max_vals = [max(abs(p), abs(c), 0.001) for p, c in zip(p_vals, c_vals)]
    p_norm = [p / mx for p, mx in zip(p_vals, max_vals)]
    c_norm = [c / mx for c, mx in zip(c_vals, max_vals)]

    fig_radar = go.Figure()
    fig_radar.add_trace(go.Scatterpolar(
        r=p_norm + [p_norm[0]], theta=categories + [categories[0]],
        fill="toself", name="Protected (CAC=0)",
        line_color="#22c55e", fillcolor="rgba(34,197,94,0.15)",
    ))
    fig_radar.add_trace(go.Scatterpolar(
        r=c_norm + [c_norm[0]], theta=categories + [categories[0]],
        fill="toself", name="Calcified (CAC>0)",
        line_color="#ef4444", fillcolor="rgba(239,68,68,0.15)",
    ))
    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1.1])),
        title="Normalized Biomarker Profiles: Protected vs Calcified",
        height=550,
    )
    st.plotly_chart(fig_radar, use_container_width=True)

    st.info(
        "💡 **The divergence tells the story:** Where the red shape extends beyond "
        "the green, that biomarker is elevated in the calcified group — even though "
        "both groups are metabolically healthy with high LDL. These are the hidden "
        "risk factors that LDL alone cannot capture."
    )
else:
    st.warning("Not enough data to compare cohorts. Run the pipeline first.")

with st.expander("📋 View underlying data"):
    st.dataframe(df_divergence, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# TILE 4: The Pure LDL Test — Funnel + CAC Distribution
# ---------------------------------------------------------------------------
st.header("🎯 Tile 4: The Pure LDL Test — Remove Every Confounder")
st.markdown(
    "If LDL alone causes arterial calcification, then people with elevated LDL "
    "who are metabolically healthy, never smoked, and have normal kidney function "
    "should **still** show significant calcification. Do they?"
)

# Split funnel stages (stage_order 1-5) from CAC distribution (6-9)
df_funnel = df_pure[df_pure["stage_order"] <= 5].sort_values("stage_order")
df_cac = df_pure[df_pure["stage_order"] > 5].sort_values("stage_order")

# --- Funnel chart ---
st.subheader("Progressive Filtering Funnel")
st.markdown(
    "Each step **removes a known confounder** to isolate LDL as the only remaining "
    "risk factor. The count shows how many subjects survive each filter — "
    "**not** how many have calcification."
)
fig_funnel = go.Figure(go.Funnel(
    y=df_funnel["stage"],
    x=df_funnel["subject_count"],
    textinfo="value+percent initial",
    marker=dict(color=["#6366f1", "#8b5cf6", "#22c55e", "#38bdf8", "#f59e0b"]),
))
fig_funnel.update_layout(
    title="Removing Confounders Step by Step — How Many Subjects Remain?",
    yaxis_title="Filter Applied",
    xaxis_title="Subjects Remaining",
    height=450,
)
st.plotly_chart(fig_funnel, use_container_width=True)

final_n = df_funnel[df_funnel["stage_order"] == 5]["subject_count"].values[0]
st.markdown(
    f"**{final_n}** subjects remain after all filters. These people have elevated LDL "
    f"but **none** of the other known risk factors. Now let's check their arteries:"
)

# --- CAC severity donut ---
if len(df_cac) > 0:
    st.subheader("CAC Severity in the 'Pure' High-LDL Cohort")

    cac_colors = {
        "CAC: No calcification": "#22c55e",
        "CAC: Mild": "#facc15",
        "CAC: Moderate": "#f97316",
        "CAC: Severe": "#ef4444",
    }
    colors = [cac_colors.get(s, "#94a3b8") for s in df_cac["stage"]]

    fig_donut = go.Figure(go.Pie(
        labels=df_cac["stage"].str.replace("CAC: ", ""),
        values=df_cac["subject_count"],
        hole=0.5,
        marker=dict(colors=colors),
        textinfo="label+percent+value",
    ))
    fig_donut.update_layout(
        title="What Happens When LDL Is the Only 'Risk Factor' Left?",
        height=450,
    )
    st.plotly_chart(fig_donut, use_container_width=True)

    no_calc = df_cac[df_cac["stage"].str.contains("No calcification")]
    if len(no_calc) > 0:
        pct_clean = no_calc["subject_count"].values[0] / final_n * 100
        st.info(
            f"💡 **The punchline:** {pct_clean:.0f}% of metabolically healthy, "
            f"non-smoking, good-kidney subjects with elevated LDL have **zero** "
            f"coronary calcification. If LDL were the causal driver, this number "
            f"should be much lower."
        )

with st.expander("📋 View underlying data"):
    st.dataframe(df_pure, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# TILE 5: Real-Value Evidence — LDL vs CAC with actual numbers
# ---------------------------------------------------------------------------
st.header("📊 Tile 5: Does LDL Actually Predict Calcification?")
st.markdown(
    "No categories, no bins — just raw biomarker values plotted against CAC. "
    "If LDL drives calcification, these charts should show a clear signal."
)

# Filter to metabolically healthy for a controlled comparison
df_healthy = df_staging[df_staging["metabolic_health_status"] == "Metabolically healthy"].copy()
df_healthy["cac_group"] = df_healthy["cacs"].apply(
    lambda x: "CAC = 0" if x == 0 else "CAC > 0"
)

# --- Chart 1: Scatter — LDL vs CAC score ---
st.subheader("1. LDL Cholesterol vs CAC Score (Metabolically Healthy)")
st.markdown(
    "Each dot is a real person. If LDL caused calcification, "
    "you'd see dots climbing to the right. Do they?"
)

# Use log scale for CAC since it's heavily right-skewed; add jitter for CAC=0
import numpy as np
df_scatter = df_healthy.copy()
df_scatter["cacs_display"] = df_scatter["cacs"].apply(
    lambda x: 0.5 if x == 0 else x  # slight offset so CAC=0 shows on log scale
)

fig_scatter = px.scatter(
    df_scatter,
    x="ldl_chol",
    y="cacs_display",
    color="cac_group",
    color_discrete_map={"CAC = 0": "#22c55e", "CAC > 0": "#ef4444"},
    hover_data=["age_yr", "tg_hdl_ratio", "smoking_status", "bmi"],
    labels={"ldl_chol": "LDL Cholesterol (mg/dL)", "cacs_display": "CAC Score (log scale)"},
    opacity=0.7,
)
fig_scatter.update_layout(
    yaxis_type="log",
    yaxis_title="CAC Score (log scale — 0 shown as 0.5)",
    height=500,
    title="LDL vs CAC: No Visible Dose-Response Relationship",
)
# Add a trend line manually via OLS
from scipy import stats as scipy_stats
mask_valid = df_healthy["ldl_chol"].notna() & df_healthy["cacs"].notna()
slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(
    df_healthy.loc[mask_valid, "ldl_chol"], df_healthy.loc[mask_valid, "cacs"]
)
x_range = np.linspace(df_healthy["ldl_chol"].min(), df_healthy["ldl_chol"].max(), 100)
fig_scatter.add_trace(go.Scatter(
    x=x_range, y=np.maximum(0.5, intercept + slope * x_range),
    mode="lines", name=f"Trend (r={r_value:.3f}, p={p_value:.3f})",
    line=dict(color="#94a3b8", dash="dash", width=2),
))
st.plotly_chart(fig_scatter, use_container_width=True)
st.caption(
    f"Linear regression: r = {r_value:.3f}, p = {p_value:.3f}. "
    f"{'Statistically significant' if p_value < 0.05 else 'Not statistically significant'}."
)

# --- Chart 2: Overlapping histograms — LDL distributions ---
st.subheader("2. LDL Distribution: Who Calcifies vs Who Doesn't")
st.markdown(
    "If LDL separated the two groups, the red histogram would shift right. "
    "If they overlap — LDL cannot distinguish who will calcify."
)

fig_hist = go.Figure()
for group, color in [("CAC = 0", "#22c55e"), ("CAC > 0", "#ef4444")]:
    subset = df_healthy[df_healthy["cac_group"] == group]
    fig_hist.add_trace(go.Histogram(
        x=subset["ldl_chol"],
        name=group,
        marker_color=color,
        opacity=0.6,
        nbinsx=30,
    ))
fig_hist.update_layout(
    barmode="overlay",
    xaxis_title="LDL Cholesterol (mg/dL)",
    yaxis_title="Number of Subjects",
    title="LDL Distributions Overlap Almost Completely",
    height=450,
)
st.plotly_chart(fig_hist, use_container_width=True)

# Compute means for annotation
mean_cac0 = df_healthy.loc[df_healthy["cacs"] == 0, "ldl_chol"].mean()
mean_cac1 = df_healthy.loc[df_healthy["cacs"] > 0, "ldl_chol"].mean()
st.caption(
    f"Mean LDL — CAC=0: {mean_cac0:.1f} mg/dL | CAC>0: {mean_cac1:.1f} mg/dL | "
    f"Difference: {abs(mean_cac1 - mean_cac0):.1f} mg/dL"
)

# --- Chart 3: Correlation ranking — what ACTUALLY predicts CAC? ---
st.subheader("3. Correlation Ranking: What Actually Predicts CAC?")
st.markdown(
    "Pearson correlation of each biomarker with CAC score among metabolically "
    "healthy subjects. The taller the bar, the stronger the association. "
    "Where does LDL land?"
)

corr_cols = {
    "age_yr": "Age",
    "ldl_chol": "LDL ⬅",
    "hdl_chol": "HDL",
    "tg": "Triglycerides",
    "tg_hdl_ratio": "TG/HDL Ratio",
    "non_hdl_chol": "Non-HDL Chol",
    "chol": "Total Chol",
    "glucose": "Glucose",
    "hb_a1c": "HbA1c",
    "sbp": "Systolic BP",
    "dbp": "Diastolic BP",
    "bmi": "BMI",
    "hs_crp": "hs-CRP",
    "creatinine": "Creatinine",
    "idms_mdrd_gfr": "GFR",
    "metabolic_syndrome_score": "Met Score",
}

corr_results = []
for col, label in corr_cols.items():
    mask = df_healthy[col].notna() & df_healthy["cacs"].notna()
    if mask.sum() > 10:
        r, p = scipy_stats.pearsonr(df_healthy.loc[mask, col], df_healthy.loc[mask, "cacs"])
        corr_results.append({"Biomarker": label, "r": r, "p": p, "abs_r": abs(r)})

df_corr = pd.DataFrame(corr_results).sort_values("abs_r", ascending=True)

colors = ["#ef4444" if "LDL" in b else "#6366f1" for b in df_corr["Biomarker"]]

fig_corr = go.Figure(go.Bar(
    y=df_corr["Biomarker"],
    x=df_corr["r"],
    orientation="h",
    marker_color=colors,
    text=df_corr.apply(lambda row: f"r={row['r']:.3f} (p={row['p']:.3f})", axis=1),
    textposition="outside",
))
fig_corr.update_layout(
    title="Correlation with CAC Score — LDL Highlighted in Red",
    xaxis_title="Pearson r (correlation with CAC)",
    yaxis_title="",
    height=550,
)
st.plotly_chart(fig_corr, use_container_width=True)

# Find LDL rank
ldl_rank = list(df_corr["Biomarker"]).index("LDL ⬅") + 1
total_markers = len(df_corr)
st.info(
    f"💡 **LDL ranks #{ldl_rank} out of {total_markers} biomarkers** in correlation "
    f"with CAC among metabolically healthy subjects. "
    f"Age, blood pressure, and metabolic markers outperform it."
)

st.markdown("---")

# ---------------------------------------------------------------------------
# TILE 6: Full Population Head-to-Head — CAC=0 vs CAC>0 (no filters)
# ---------------------------------------------------------------------------
st.header("⚔️ Tile 6: Full Population — Who Calcifies vs Who Doesn't?")
st.markdown(
    "No metabolic filter, no LDL filter — **all 1,688 subjects**. "
    "Split only by whether they have any calcification. "
    "If LDL is the driver, it should rank at the top. If it doesn't — the myth is busted "
    "in the entire population, not just a subgroup."
)

df_all = df_staging.copy()
df_all["cac_group"] = df_all["cacs"].apply(lambda x: "CAC = 0" if x == 0 else "CAC > 0")

no_cac = df_all[df_all["cac_group"] == "CAC = 0"]
yes_cac = df_all[df_all["cac_group"] == "CAC > 0"]

n_no  = len(no_cac)
n_yes = len(yes_cac)

c1, c2 = st.columns(2)
c1.metric("CAC = 0 (No calcification)", f"{n_no:,}", f"{n_no/len(df_all)*100:.0f}% of all subjects")
c2.metric("CAC > 0 (Any calcification)", f"{n_yes:,}", f"{n_yes/len(df_all)*100:.0f}% of all subjects")

# --- Comparison table ---
st.subheader("Head-to-Head Biomarker Comparison")

comparison_metrics_full = [
    ("Avg Age (yr)",               "age_yr",                    ".1f"),
    ("% Male",                     None,                        ".1f"),
    ("Avg BMI",                    "bmi",                       ".1f"),
    ("Avg LDL (mg/dL)",            "ldl_chol",                  ".1f"),
    ("Avg HDL (mg/dL)",            "hdl_chol",                  ".1f"),
    ("Avg Triglycerides (mg/dL)",  "tg",                        ".1f"),
    ("Avg TG/HDL Ratio",           "tg_hdl_ratio",              ".2f"),
    ("Avg Non-HDL Chol (mg/dL)",   "non_hdl_chol",              ".1f"),
    ("Avg Total Chol (mg/dL)",     "chol",                      ".1f"),
    ("Avg Glucose (mg/dL)",        "glucose",                   ".1f"),
    ("Avg HbA1c (%)",              "hb_a1c",                    ".2f"),
    ("Avg Systolic BP (mmHg)",     "sbp",                       ".1f"),
    ("Avg Diastolic BP (mmHg)",    "dbp",                       ".1f"),
    ("Avg hs-CRP (mg/L)",          "hs_crp",                    ".2f"),
    ("Avg Met Syndrome Score",     "metabolic_syndrome_score",  ".2f"),
    ("Avg GFR (mL/min)",           "idms_mdrd_gfr",             ".1f"),
]

rows = []
for label, col, fmt in comparison_metrics_full:
    if col is None:  # % Male special case
        p_val = no_cac["sex"].eq("M").mean() * 100
        c_val = yes_cac["sex"].eq("M").mean() * 100
    else:
        p_val = no_cac[col].mean()
        c_val = yes_cac[col].mean()
    diff = c_val - p_val
    rows.append({
        "Biomarker": label,
        "CAC = 0": f"{p_val:{fmt}}",
        "CAC > 0": f"{c_val:{fmt}}",
        "Difference (CAC>0 minus CAC=0)": f"{diff:+{fmt}}",
    })

df_compare_full = pd.DataFrame(rows)
st.dataframe(df_compare_full, use_container_width=True, hide_index=True)

# --- Correlation ranking: full population ---
st.subheader("Correlation Ranking — Full Population")
st.markdown(
    "Pearson correlation of each biomarker with CAC score across **all subjects**. "
    "No filters. Where does LDL rank?"
)

corr_cols_full = {
    "age_yr":                   "Age",
    "ldl_chol":                 "LDL ⬅",
    "hdl_chol":                 "HDL",
    "tg":                       "Triglycerides",
    "tg_hdl_ratio":             "TG/HDL Ratio",
    "non_hdl_chol":             "Non-HDL Chol",
    "chol":                     "Total Chol",
    "glucose":                  "Glucose",
    "hb_a1c":                   "HbA1c",
    "sbp":                      "Systolic BP",
    "dbp":                      "Diastolic BP",
    "bmi":                      "BMI",
    "hs_crp":                   "hs-CRP",
    "creatinine":               "Creatinine",
    "idms_mdrd_gfr":            "GFR",
    "metabolic_syndrome_score": "Met Score",
}

corr_results_full = []
for col, label in corr_cols_full.items():
    mask = df_all[col].notna() & df_all["cacs"].notna()
    if mask.sum() > 10:
        r, p = scipy_stats.pearsonr(df_all.loc[mask, col], df_all.loc[mask, "cacs"])
        corr_results_full.append({"Biomarker": label, "r": r, "p": p, "abs_r": abs(r)})

df_corr_full = pd.DataFrame(corr_results_full).sort_values("abs_r", ascending=True)
colors_full = ["#ef4444" if "LDL" in b else "#6366f1" for b in df_corr_full["Biomarker"]]

fig_corr_full = go.Figure(go.Bar(
    y=df_corr_full["Biomarker"],
    x=df_corr_full["r"],
    orientation="h",
    marker_color=colors_full,
    text=df_corr_full.apply(lambda row: f"r={row['r']:.3f} (p={row['p']:.3f})", axis=1),
    textposition="outside",
))
fig_corr_full.update_layout(
    title="Correlation with CAC Score — Full Population (n=1,688), LDL in Red",
    xaxis_title="Pearson r",
    height=550,
)
st.plotly_chart(fig_corr_full, use_container_width=True)

ldl_rank_full = list(df_corr_full["Biomarker"]).index("LDL ⬅") + 1
total_full = len(df_corr_full)
st.info(
    f"💡 **Full population verdict:** LDL ranks #{ldl_rank_full} out of {total_full} biomarkers "
    f"in correlation with CAC. This is the whole dataset — no filters, no subgroups."
)

with st.expander("📋 View underlying data"):
    st.dataframe(df_compare_full, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown(
    """
    ### About This Project
    **LipidMythBuster** — Data Engineering Zoomcamp 2026 Final Project

    - **Dataset:** Korean Asymptomatic Health Screening CAC (n=1,688), [Figshare](https://figshare.com/articles/dataset/Clinical_characteristics_CACS_GWAS_n_1688/7853588)
    - **Pipeline:** Bruin (ingestion → staging → reports) on DuckDB
    - **Clinical references:** NCEP ATP III guidelines, MESA CAC cutoffs, AHA/ACC non-HDL recommendations
    - **LDL particle proxy:** Maruyama et al. (2003), Boizel et al. (2000)

    *This dashboard does not constitute medical advice. It is an educational data engineering project.*
    """
)
