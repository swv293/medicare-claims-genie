# Medicaid Clinical Quality Measures — Genie Room Demo

An end-to-end, open-source demo of [Databricks AI/BI Genie](https://docs.databricks.com/aws/en/genie/) on a Medicaid clinical quality data warehouse. Ask questions about HEDIS measures, enrollment, claims costs, and provider performance in plain English — Genie writes the SQL, the metric view enforces the math, and Unity Catalog enforces the governance.

## What is a Genie Space?

A [Genie space](https://docs.databricks.com/aws/en/genie/) is a curated natural-language interface over a set of Unity Catalog tables and metric views. Business users type questions; Genie generates SQL, executes it on a serverless SQL warehouse, and returns answers as tables, charts, or values. The "curation" lives in the space's `serialized_space` — a JSON document that bundles:

- **Sample questions** — chips that appear in the room as starter prompts
- **Text instructions** — domain context (jargon, calculation rules, do/don't guidance)
- **SQL snippets** — reusable filters, expressions, and measures Genie can compose
- **Join specs** — declarative relationships between tables so Genie joins them correctly
- **Benchmarks** — example question-to-SQL pairs that act as few-shot examples

This repo builds **all of that** for a Medicaid clinical use case: 13 sample questions, 1 instruction block, 10 SQL snippets, 10 join specs, and 10 benchmark queries (5 standard aggregation + 5 window-function patterns).

## Why this demo is interesting

Two things in this build are worth a closer look:

### `mv_quality_performance` — a metric view as the semantic layer

The HEDIS performance-rate formula is ugly:

```
ROUND(COUNT(CASE WHEN in_numerator AND NOT exclusion_applied THEN 1 END) * 100.0
      / NULLIF(COUNT(CASE WHEN in_denominator AND NOT exclusion_applied THEN 1 END), 0), 2)
```

If every analyst writes that by hand, half of them will write it wrong. The [metric view](https://docs.databricks.com/aws/en/metric-views/) defines it once in YAML and exposes it as `MEASURE(performance_rate)`. Genie generates `MEASURE(performance_rate)` instead of trying to reconstruct the formula — every answer uses the same, audit-correct math.

The metric view also pre-joins the four dimensions onto `fact_quality_events`, so any of 20+ dimensions (`measure_name`, `county_name`, `provider_type`, `aid_category`, `gender`, `race_ethnicity`, `quarter`, etc.) is one `GROUP BY` away. Eight `MEASURE()` expressions × 20+ dimensions = a wide cube without writing a join.

### `dim_measure` — the small reference table that earns its keep

Eighteen rows. Three boolean flags (`high_priority_flag`, `star_rating_flag`, plus `reporting_direction` enum). One `regulatory_threshold` per measure. That's enough metadata to power some of the most useful Genie answers in the space:

- **"Which measures are at risk?"** uses `regulatory_threshold` + `reporting_direction` to flip the `<` vs `>` comparison correctly for "Higher is Better" (most measures) vs "Lower is Better" (CDC-HbA1c, Plan All-Cause Readmissions).
- **"Show only the high-priority measures"** picks up the `high_priority_flag` snippet and filters automatically.
- **"How are diabetes measures trending?"** uses `measure_category` for a clean group-by.

Small, well-commented, well-tagged dimension tables are the unsung heroes of every good Genie space.

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
         └── 8 MEASURE() aggregates + 20+ dimensions
```

| Table | Type | Rows | Description |
|-------|------|------|-------------|
| `dim_member` | Dimension | 1,000 | Medicaid enrollee demographics, chronic conditions, aid category |
| `dim_county` | Dimension | 2,500 | County FIPS codes, state, region, urban/rural classification |
| `dim_provider` | Dimension | 500 | Provider NPI, type (PCP, FQHC, BH, etc.), specialty |
| `dim_measure` | Dimension | 18 | HEDIS/CMS quality measure definitions with thresholds |
| `fact_quality_events` | Fact | 10,000 | Member × measure × year with in_denominator/in_numerator flags |
| `fact_enrollment` | Fact | 3,000 | Monthly enrollment snapshots by member |
| `fact_claims` | Fact | 10,000 | Claims with ICD-10 dx_codes, CPT proc_codes, paid amounts |
| `mv_quality_performance` | Metric View | — | Pre-joined quality analytics with `MEASURE()` functions |

---

## Quickstart — recreate this Genie space in your own workspace

You have two paths. Both end at the same Genie space.

### Path A — Databricks Free Edition (zero cost, ~10 min)

[Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) is a no-credit-card, single-user workspace with a serverless SQL warehouse pre-provisioned. It's ideal for trying this demo without involving your IT team.

1. **Sign up**: [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition). Pick AWS or Azure (either works).
2. **Get your SQL warehouse ID**: in the left nav click **SQL Warehouses**, click the default warehouse, and copy the **ID** from the URL (`/sql/warehouses/<warehouse_id>`).
3. **Pick a catalog and schema**: Free Edition gives you a `workspace` catalog. Use schema name `medicaid_clinical`.
4. **Import the notebook**: in your workspace, click **Workspace** → click your username → **⋯ menu** → **Import** → upload `notebooks/medicaid_clinical_setup.py`. ([How to import notebooks](https://docs.databricks.com/aws/en/notebooks/notebook-export-import))
5. **Edit two lines at the top of the notebook**:
   ```python
   CATALOG = "workspace"          # or your Free Edition catalog
   SCHEMA  = "medicaid_clinical"
   ```
   And in Step 9.1, set `WAREHOUSE_ID` to the id you copied.
6. **Click "Run all"**. The notebook creates the schema, generates 30K rows of synthetic data, builds the metric view, applies governance tags, then creates a Genie space and prints its URL.
7. **Open the URL**, ask "Which measures are at risk of not meeting regulatory thresholds?" — you should see a chart in seconds.

That's it. Re-running the notebook is safe — it PATCHes the same Genie space instead of creating a new one (the `space_id` is persisted in `config_genie`).

### Path B — Your existing Databricks workspace

Same as Path A, but with your own catalog/schema. Required permissions:
- `USE CATALOG` + `CREATE SCHEMA` on a catalog you own
- A SQL warehouse (Serverless or Pro) you can run queries on
- `databricks.genie:create_space` (any user with workspace access in most workspaces)

Open `notebooks/medicaid_clinical_setup.py`, set `CATALOG`, `SCHEMA`, and `WAREHOUSE_ID`, then **Run all**.

### Path C — CLI scripts (for CI/CD or scripted environments)

Useful if you want to build the space from a build pipeline rather than an interactive notebook.

```bash
# 1. Authenticate the Databricks CLI
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# 2. Edit PROFILE, WAREHOUSE, CATALOG, SCHEMA in each script (top of file)

# 3. Generate data and load tables
python generate_data.py
python execute_sql.py create_tables.sql
python execute_sql.py insert_data.sql
python execute_sql.py apply_tags.sql
python execute_sql.py create_metric_view.sql

# 4. Create the Genie space (prints space_id and URL)
python create_genie_space.py

# 5. Apply structured config: joins, snippets, benchmarks
#    NOTE: copy the space_id from step 4 into update_genie_space.py and add_window_functions.py
python update_genie_space.py
python add_window_functions.py        # MUST run AFTER update_genie_space.py
```

> ⚠️ **Order matters**: `update_genie_space.py` overwrites `benchmarks` with the 5 standard ones, so you must run `add_window_functions.py` *after* it to layer in the 5 window-function benchmarks. The notebook (Path A/B) builds all 10 in a single PATCH, so this ordering trap doesn't apply there.

---

## Try it in 60 seconds

Open the Genie space URL and ask:

1. **"What are our Medicaid enrollment numbers by county?"** — exercises the basic star-schema join
2. **"Which measures are at risk of not meeting regulatory thresholds?"** — exercises `dim_measure.regulatory_threshold` + `reporting_direction` for direction-aware comparison
3. **"Rank providers by their quality measure performance rate"** — exercises window function generation (taught via benchmarks)

For each, click "Show generated SQL" and notice that:
- Question (2) wraps measures in `MEASURE(...)` — the metric view in action
- Question (3) emits a `RANK() OVER(PARTITION BY ...)` even though the metric view itself doesn't define windows — Genie learned the pattern from the benchmark queries

## Demo primer (≈3 minutes)

| Beat | Time | What to do |
|---|---|---|
| Open | 30s | "This is the data warehouse a payer's HEDIS team would use. We've connected it to Genie so business users can ask questions in English." |
| Basics | 60s | Ask "Medicaid enrollment by county" — show the result, then click into the SQL. Genie picked the right join. |
| Domain intelligence | 60s | Ask "Which measures are at risk?" — point out the `dim_measure.regulatory_threshold` filter and the direction-aware comparison. |
| Advanced analytics | 60s | Ask "Rank providers by performance rate" — `RANK() OVER` was generated despite the metric view having no windows. Benchmarks taught it. |
| Governance | 30s | Show PHI/PII tags in Unity Catalog. Same query, different users — column masking and row filters apply automatically. |
| Close | 30s | "Days, not months. The schema is standard. The Genie config is one notebook. The advanced patterns came from a few example queries — no fine-tuning." |

## Best Practices Implemented

- **Metric view as semantic layer.** `mv_quality_performance` defines all eight measures (denominator, numerator, performance_rate, gap_to_threshold, total_events, exclusion_count, distinct_members, distinct_providers) once. Every Genie answer uses the same math.
- **Benchmark queries as few-shot examples.** Ten benchmarks — five standard aggregation patterns and five window-function patterns (RANK, LAG, NTILE, running SUM/AVG, PERCENT_RANK). Window-function generation is *taught*, not fine-tuned.
- **Domain-specific instructions.** Healthcare jargon (HEDIS, CMS Core Set, TANF/SSI/CHIP, PMPM, MCO/FFS), the exact performance-rate formula, the `Higher is Better` vs `Lower is Better` rule, and a `metric view vs base tables` decision rule.
- **SQL snippets for reusable logic.** Filters (`active members only`, `eligible for measure`, `high priority measures`, `star rating measures`), expressions (`measurement_quarter`, `member_age`), and measures (`performance_rate`, `total_paid`, `pmpm_cost`).
- **Join specs.** Ten declarative joins with backtick-quoted aliases and `--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--` annotations so Genie generates correct cardinality.
- **Governance tags on every table.** `phi=true`, `pii=true`, `hipaa=true`, `data_classification=...` on sensitive columns. Tags drive Unity Catalog [column masks](https://docs.databricks.com/aws/en/tables/column-mask) and [row filters](https://docs.databricks.com/aws/en/tables/row-and-column-filters).
- **Row-filter cascade pattern.** The notebook ships an optional Step 10 that attaches a row filter to `dim_member` and demonstrates that it cascades automatically through views and the metric view into Genie.
- **Descriptive column comments.** Every column has a `COMMENT` in the DDL — Genie reads them to disambiguate when picking columns.

## Quality Measures (18 Seeded)

| Category | Measures |
|----------|----------|
| Diabetes | HbA1c Poor Control, Eye Exam, Kidney Health Evaluation |
| Cardiovascular | Blood Pressure Control |
| Preventive | Breast / Cervical / Colorectal Cancer Screening, Adult Immunization |
| Child Health | Well-Child Visits (3-21), Childhood Immunization Status |
| Behavioral Health | MH Follow-Up (Inpatient), MH Follow-Up (ED), Antidepressant Medication Management, SUD Initiation & Engagement |
| Respiratory | Asthma Medication Ratio |
| Maternal Health | Prenatal & Postpartum Care |
| Utilization | Plan All-Cause Readmissions |

## Sample Questions

**Standard analytics**
1. What are our Medicaid enrollment numbers by county?
2. Show me clinical quality metrics for the current quarter
3. Which measures are at risk of not meeting regulatory thresholds?
4. Compare this year's performance vs last year by quality measure
5. How are we performing on all diabetes-related quality measures?
6. Show behavioral health follow-up rates by quarter
7. Which providers have the highest quality measure compliance rates?
8. Show enrollment trends by aid category over time

**Window-function analytics**
9. Rank providers by their quality measure performance rate
10. Show quarter-over-quarter performance trend for each measure
11. Which providers are in the bottom quartile for quality performance?
12. Show cumulative enrollment growth with a 3-month rolling average
13. What percentile does each county rank in for diabetes measure performance?

## Files

| File | Purpose |
|------|---------|
| `notebooks/medicaid_clinical_setup.py` | **One-notebook path**: schema + tables + data + metric view + tags + Genie space (idempotent) + optional row filter |
| `create_tables.sql` | DDL with column comments and governance tags |
| `apply_tags.sql` | PHI/PII/domain tag statements |
| `create_metric_view.sql` | Metric view YAML definition (8 measures, 20+ dimensions) |
| `generate_data.py` | Synthetic data generator (deterministic, seed=42, stdlib-only) |
| `execute_sql.py` | SQL execution utility via Databricks CLI |
| `create_genie_space.py` | CLI alternative — creates the Genie space |
| `update_genie_space.py` | CLI alternative — adds joins, snippets, standard benchmarks |
| `add_window_functions.py` | CLI alternative — adds 5 window-function benchmarks |
| `insert_data.sql` | Generated INSERT statements (created by `generate_data.py`) |

## Useful links

- [Genie space documentation](https://docs.databricks.com/aws/en/genie/)
- [Genie REST API reference](https://docs.databricks.com/api/workspace/genie)
- [Metric views](https://docs.databricks.com/aws/en/metric-views/)
- [Unity Catalog row filters and column masks](https://docs.databricks.com/aws/en/tables/row-and-column-filters)
- [Databricks Free Edition signup](https://www.databricks.com/learn/free-edition)
- [Databricks CLI install](https://docs.databricks.com/aws/en/dev-tools/cli/install)
