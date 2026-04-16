# Medicaid Clinical Quality Measures — Genie Room Demo

An end-to-end demo environment for Databricks AI/BI Genie, built on a Medicaid clinical quality data warehouse. Ask questions about HEDIS measures, enrollment trends, claims costs, and provider performance in plain English — Genie writes the SQL.

## What This Demo Shows

This Genie room demonstrates how a healthcare payer's quality team can get instant, governed, natural-language access to clinical quality analytics without writing SQL. It covers:

- **Standard analytics**: Enrollment by county, quality measure rates, at-risk measures, year-over-year comparisons
- **Advanced window functions**: Provider rankings (RANK), quarter-over-quarter trends (LAG), performance quartiles (NTILE), cumulative enrollment (running SUM), county percentiles (PERCENT_RANK)
- **Metric view semantics**: Pre-defined HEDIS-compliant rate formulas via MEASURE() functions ensure calculation consistency
- **Governance**: PHI/PII tagging, column-level classification, Unity Catalog enforcement on every query Genie generates

## Architecture

```
                       dim_measure (18 rows)
                            |
                       measure_id
                            |
dim_county ---county_fips--- fact_quality_events ---provider_npi--- dim_provider
(2,500)                      (10,000)                                (500)
                             member_id
                                |
                            dim_member (1,000)
                                |
                      fact_enrollment (3,000)
                      fact_claims (10,000)

         mv_quality_performance (metric view)
         └── pre-joins all 4 dimensions to fact_quality_events
         └── 8 MEASURE() aggregates + 20 dimensions
```

| Table | Type | Rows | Description |
|-------|------|------|-------------|
| `dim_member` | Dimension | 1,000 | Medicaid enrollee demographics, chronic conditions, aid category |
| `dim_county` | Dimension | 2,500 | County FIPS codes, state, region, urban/rural classification |
| `dim_provider` | Dimension | 500 | Provider NPI, type (PCP, FQHC, BH, etc.), specialty |
| `dim_measure` | Dimension | 18 | HEDIS/CMS quality measure definitions with thresholds |
| `fact_quality_events` | Fact | 10,000 | Member x measure x year with in_denominator/in_numerator flags |
| `fact_enrollment` | Fact | 3,000 | Monthly enrollment snapshots by member |
| `fact_claims` | Fact | 10,000 | Claims with ICD-10 dx_codes, CPT proc_codes, paid amounts |
| `mv_quality_performance` | Metric View | — | Pre-joined quality analytics with MEASURE() functions |

## Setup Guide

### Prerequisites

- A Databricks workspace (Free Trial, Community Edition, or any paid tier)
- A SQL warehouse (Serverless recommended; Pro works too)
- A catalog and schema you can write to (or permissions to create one)
- Databricks CLI installed and authenticated (`databricks auth login`)

### Step 1: Configure Your Environment

Edit the catalog and schema names in the scripts to match your workspace. The default is `serverless_stable_swv01_catalog.medicaid_clinical` — you'll want to change this.

**Files to update:**

| File | Variable(s) to change |
|------|----------------------|
| `generate_data.py` | `CATALOG`, `SCHEMA` (line 14-15) |
| `execute_sql.py` | `PROFILE`, `WAREHOUSE` (line 12-13) |
| `create_genie_space.py` | `PROFILE`, `WAREHOUSE` (line 9-10) |
| `update_genie_space.py` | `PROFILE`, `WAREHOUSE`, `SPACE_ID` (line 12-14) |
| `add_window_functions.py` | `PROFILE`, `WAREHOUSE`, `SPACE_ID` (line 12-14) |
| `create_tables.sql` | Replace all `serverless_stable_swv01_catalog.medicaid_clinical` references |
| `create_metric_view.sql` | Replace all `serverless_stable_swv01_catalog.medicaid_clinical` references |

**Tip**: Use find-and-replace across all `.sql` and `.py` files:
```bash
# Replace catalog.schema references
find . -name '*.sql' -o -name '*.py' | xargs sed -i '' \
  's/serverless_stable_swv01_catalog\.medicaid_clinical/YOUR_CATALOG.YOUR_SCHEMA/g'
```

### Step 2: Create Tables and Load Data

**Option A: Databricks Notebook (recommended for quick setup)**

1. Import `notebooks/medicaid_clinical_setup.py` into your workspace
2. Update the `CATALOG` variable at the top of the notebook
3. Attach to your SQL warehouse or compute cluster
4. Run all cells — this creates tables, generates data, creates the metric view, and applies governance tags in one go

**Option B: CLI Scripts (for automation or CI/CD)**

```bash
# 1. Generate synthetic data (creates insert_data.sql)
python generate_data.py

# 2. Create the tables
python execute_sql.py create_tables.sql

# 3. Load the data
python execute_sql.py insert_data.sql

# 4. Apply PHI/PII governance tags
python execute_sql.py apply_tags.sql

# 5. Create the metric view
python execute_sql.py create_metric_view.sql
```

### Step 3: Create the Genie Space

```bash
# Create the Genie space with sample questions, instructions, and benchmarks
python create_genie_space.py
```

This outputs the Genie Space URL. Save it — you'll need it for the next steps and for sharing.

### Step 4: Add Structured Configuration

```bash
# Add join specs, SQL snippets, and benchmark queries
python update_genie_space.py
```

### Step 5: Add Window Function Benchmarks

```bash
# Add 5 window function benchmarks and sample questions
python add_window_functions.py
```

### Step 6: Verify

Open the Genie Space URL in your browser. You should see 13 sample questions. Try asking:

1. "What are our Medicaid enrollment numbers by county?" — validates basic table joins
2. "Which measures are at risk of not meeting regulatory thresholds?" — validates metric view + domain logic
3. "Rank providers by their quality measure performance rate" — validates window function generation

## Best Practices Implemented

This Genie room follows several best practices that improve answer quality. Use these patterns when building your own spaces.

### 1. Metric View as Semantic Layer

The `mv_quality_performance` metric view defines measures (denominator, numerator, performance_rate, gap_to_threshold) in YAML. This means:

- The HEDIS rate formula is defined **once** and consumed consistently everywhere
- Business users can't accidentally compute rates wrong
- Genie wraps measures in `MEASURE()` and groups by the dimensions it selects

**Trade-off**: Metric views only support aggregate expressions — no window functions. We use a dual approach: metric view for standard analytics, base tables for window function queries.

### 2. Benchmark Queries as Few-Shot Examples

The 10 benchmark queries are the most powerful teaching tool. They give Genie example question-to-SQL pairs to learn from:

- 5 standard aggregation patterns (enrollment counts, quality metrics, at-risk measures, YoY comparison, claims cost)
- 5 window function patterns (RANK, LAG, NTILE, running SUM/AVG, PERCENT_RANK)

When Genie sees a novel question, it references these benchmarks to select the right SQL pattern. Adding the 5 window function benchmarks immediately enabled Genie to generate window function SQL for questions it had never seen.

### 3. Domain-Specific Instructions

The text instructions encode healthcare semantics that Genie can't infer from table schemas alone:

- What "at risk" means for Higher vs. Lower is Better measures
- HEDIS and CMS Core Set jargon definitions
- Aid category explanations (TANF, SSI, CHIP, Expansion Adult)
- The correct performance rate formula with exclusion handling
- When to use the metric view vs. base tables

### 4. SQL Snippets for Reusable Logic

Three types of SQL snippets provide Genie with reusable building blocks:

- **Filters**: "active members only", "eligible for measure", "high priority measures", "star rating measures"
- **Expressions**: measurement_quarter formatting, member age calculation, enrollment month labels
- **Measures**: performance_rate, total_paid, PMPM cost formulas

### 5. Governance Tags on Every Table

All tables carry:

- **Table-level tags**: domain, data_classification, contains_phi, contains_pii, hipaa
- **Column-level tags**: phi=true, pii=true on sensitive fields (member_id, date_of_birth, zip_code, dx_codes, etc.)

These tags drive Unity Catalog column masking and row-level security policies. When Genie generates SQL, it runs through the same governance layer — users without PHI access see masked values.

### 6. Descriptive Column Comments

Every column has a `COMMENT` in the DDL. Genie reads these comments to understand what columns mean and when to use them. Good column comments are often the difference between Genie selecting the right column and picking the wrong one.

## Demo Primer

Use this as a quick guide for walking someone through the Genie room. For a full scripted talk track, see the separate talk track documents (not in this repo).

**Opening** (30s): "This is a Medicaid clinical quality data warehouse — the same star schema your HEDIS team would use. We've connected it to a Genie room so business users can ask questions in plain English."

**Show the basics** (60s): Ask "What are our Medicaid enrollment numbers by county?" — show results, then show the generated SQL. Point out that Genie selected the right tables, joins, and aggregations.

**Show domain intelligence** (60s): Ask "Which measures are at risk of not meeting regulatory thresholds?" — highlight that Genie understands Higher vs. Lower is Better measures and applies different threshold logic for each.

**Show advanced analytics** (60s): Ask "Rank providers by their quality measure performance rate" — show the RANK() OVER window function in the generated SQL. Follow up with "Which providers are in the bottom quartile?" to show NTILE.

**Show governance** (30s): Navigate to Unity Catalog, show the PHI/PII tags on dim_member. Explain that Genie's queries run through the same governance layer — column masking and row-level security apply automatically.

**Close** (30s): "This took days to set up, not months. The data model is standard — your team already has one. The Genie space configuration is a Python script. And the advanced SQL patterns — window functions, metric views — are taught through benchmark examples, not model fine-tuning."

## Quality Measures (18 Seeded)

| Category | Measures |
|----------|----------|
| Diabetes | HbA1c Poor Control, Eye Exam, Kidney Health Evaluation |
| Cardiovascular | Blood Pressure Control |
| Preventive | Breast Cancer Screening, Cervical Cancer Screening, Colorectal Cancer Screening, Adult Immunization |
| Child Health | Well-Child Visits (3-21), Childhood Immunization Status |
| Behavioral Health | MH Follow-Up (Inpatient), MH Follow-Up (ED), Antidepressant Medication Management, SUD Initiation & Engagement |
| Respiratory | Asthma Medication Ratio |
| Maternal Health | Prenatal & Postpartum Care |
| Utilization | Plan All-Cause Readmissions |

## Sample Questions

### Standard Analytics
1. What are our Medicaid enrollment numbers by county?
2. Show me clinical quality metrics for the current quarter
3. Which measures are at risk of not meeting regulatory thresholds?
4. Compare this year's performance vs last year by quality measure
5. How are we performing on all diabetes-related quality measures?
6. Show behavioral health follow-up rates by quarter
7. Which providers have the highest quality measure compliance rates?
8. Show enrollment trends by aid category over time

### Window Function Analytics
9. Rank providers by their quality measure performance rate
10. Show quarter-over-quarter performance trend for each measure
11. Which providers are in the bottom quartile for quality performance?
12. Show cumulative enrollment growth with a 3-month rolling average
13. What percentile does each county rank in for diabetes measure performance?

## Files

| File | Purpose |
|------|---------|
| `notebooks/medicaid_clinical_setup.py` | Self-contained Databricks notebook (tables + data + metric view + tags) |
| `create_tables.sql` | DDL with column descriptions and governance tags |
| `insert_data.sql` | Generated INSERT statements (created by generate_data.py) |
| `apply_tags.sql` | PHI/PII/domain tag statements |
| `create_metric_view.sql` | Metric view YAML definition with 8 measures and 20 dimensions |
| `generate_data.py` | Synthetic data generator (deterministic, seed=42) |
| `execute_sql.py` | SQL execution utility via Databricks CLI |
| `create_genie_space.py` | Creates the Genie space with sample questions and instructions |
| `update_genie_space.py` | Adds join specs, SQL snippets, and benchmark queries |
| `add_window_functions.py` | Adds 5 window function benchmarks and sample questions |
