# Medicaid Clinical Quality Measures - Data Warehouse & Genie Space

A production-ready Medicaid clinical data warehouse built on Databricks Delta Lake with an AI/BI Genie space for natural language querying. Supports HEDIS and CMS Core Set quality measure reporting with year-over-year performance comparisons.

## Architecture

Star schema design with a central fact table (`fact_quality_events`) surrounded by dimension tables:

| Table | Type | Rows | Description |
|-------|------|------|-------------|
| `dim_member` | Dimension | 1,000 | Medicaid enrollee demographics |
| `dim_county` | Dimension | 2,500 | County reference (FIPS codes) |
| `dim_provider` | Dimension | 500 | Provider registry (NPI) |
| `dim_measure` | Dimension | 18 | HEDIS/CMS quality measure definitions |
| `fact_quality_events` | Fact | 10,000 | Member x measure x year events |
| `fact_enrollment` | Fact | 3,000 | Monthly enrollment snapshots |
| `fact_claims` | Fact | 10,000 | Claims with ICD-10/CPT codes |
| `mv_quality_performance` | Metric View | - | Flexible quality measure analytics with MEASURE() functions |

## Quality Measures (18 Seeded)

Covers key HEDIS MY 2025 and CMS 2025 Core Set measures across:
- **Diabetes**: HbA1c, Eye Exam, Kidney Health
- **Cardiovascular**: Blood Pressure Control
- **Preventive**: Breast/Cervical/Colorectal Cancer Screening, Adult Immunization
- **Child Health**: Well-Child Visits, Childhood Immunization
- **Behavioral Health**: MH Follow-Up (Inpatient/ED), Antidepressant Management, SUD Treatment
- **Respiratory**: Asthma Medication Ratio
- **Maternal Health**: Prenatal/Postpartum Care
- **Utilization**: All-Cause Readmissions

## Governance

All tables include:
- **Table and column descriptions** for Unity Catalog data discovery
- **PHI/PII tags** for governance policies (HIPAA compliance)
- **Domain tags** for organizational classification

## Quick Start

### Option 1: Run the Databricks Notebook
1. Import `notebooks/medicaid_clinical_setup.py` into your Databricks workspace
2. Update the `CATALOG` variable at the top
3. Run all cells

### Option 2: Run SQL Scripts Locally
```bash
# Generate synthetic data
python generate_data.py

# Execute DDL
python execute_sql.py create_tables.sql

# Load data
python execute_sql.py insert_data.sql

# Apply tags
python execute_sql.py apply_tags.sql

# Create metric view
python execute_sql.py create_metric_view.sql
```

## Files

| File | Purpose |
|------|---------|
| `notebooks/medicaid_clinical_setup.py` | Self-contained Databricks notebook |
| `create_tables.sql` | DDL with descriptions |
| `apply_tags.sql` | PHI/PII tag statements |
| `create_metric_view.sql` | Metric view with MEASURE() functions |
| `generate_data.py` | Synthetic data generator |
| `execute_sql.py` | SQL execution utility |
| `create_genie_space.py` | Genie space creation script |

## Benchmark Queries (Genie Space)

The Genie space includes 5 benchmark queries registered as `example_question_sqls`. These serve as reference SQL for Genie to learn from and as test cases for evaluating response accuracy.

| # | Question | Tables/Views Used |
|---|----------|-------------------|
| 1 | "What are our Medicaid enrollment numbers by county?" | `fact_enrollment` JOIN `dim_county` |
| 2 | "Show me clinical quality metrics for the current quarter" | `mv_quality_performance` (metric view) |
| 3 | "Which measures are at risk of not meeting regulatory thresholds?" | `mv_quality_performance` (metric view) |
| 4 | "Compare this year's performance vs last year by quality measure" | `mv_quality_performance` (metric view) |
| 5 | "Show claims cost breakdown by claim type and aid category" | `fact_claims` JOIN `dim_member` |

## Sample Questions (Genie Space)

- "What are our Medicaid enrollment numbers by county?"
- "Show me clinical quality metrics for the current quarter"
- "Which measures are at risk of not meeting regulatory thresholds?"
- "Compare this year's performance vs last year by quality measure"
- "How are we performing on all diabetes-related quality measures?"
- "Show behavioral health follow-up rates by quarter"
- "Which providers have the highest quality measure compliance rates?"
- "Show enrollment trends by aid category over time"
