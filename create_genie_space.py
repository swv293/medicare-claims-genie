"""Create a Genie space for Medicaid Clinical Quality Measures."""
import json
import subprocess
import os
import ssl
import urllib.request
import urllib.error

PROFILE = "fe-vm-fevm-serverless-stable-swv01"
WAREHOUSE = "084543d48aafaeb2"

# Get auth info
env_out = subprocess.run(
    ["databricks", "auth", "env", f"--profile={PROFILE}"],
    capture_output=True, text=True
)
env_data = json.loads(env_out.stdout)
HOST = env_data["env"]["DATABRICKS_HOST"]

token_out = subprocess.run(
    ["databricks", "auth", "token", f"--profile={PROFILE}"],
    capture_output=True, text=True
)
TOKEN = json.loads(token_out.stdout)["access_token"]

# Build the serialized space
serialized_space = {
    "version": 2,
    "config": {
        "sample_questions": [
            {
                "id": "a0000000000000000000000000000001",
                "question": ["What are our Medicaid enrollment numbers by county?"]
            },
            {
                "id": "a0000000000000000000000000000002",
                "question": ["Show me clinical quality metrics for the current quarter"]
            },
            {
                "id": "a0000000000000000000000000000003",
                "question": ["Which measures are at risk of not meeting regulatory thresholds?"]
            },
            {
                "id": "a0000000000000000000000000000004",
                "question": ["Compare this year's performance vs last year by quality measure"]
            },
            {
                "id": "a0000000000000000000000000000005",
                "question": ["How are we performing on all diabetes-related quality measures?"]
            },
            {
                "id": "a0000000000000000000000000000006",
                "question": ["Show behavioral health follow-up rates by quarter"]
            },
            {
                "id": "a0000000000000000000000000000007",
                "question": ["Which providers have the highest quality measure compliance rates?"]
            },
            {
                "id": "a0000000000000000000000000000008",
                "question": ["Show enrollment trends by aid category over time"]
            }
        ]
    },
    "data_sources": {
        "tables": [
            {"identifier": "serverless_stable_swv01_catalog.medicaid_clinical.dim_county"},
            {"identifier": "serverless_stable_swv01_catalog.medicaid_clinical.dim_measure"},
            {"identifier": "serverless_stable_swv01_catalog.medicaid_clinical.dim_member"},
            {"identifier": "serverless_stable_swv01_catalog.medicaid_clinical.dim_provider"},
            {"identifier": "serverless_stable_swv01_catalog.medicaid_clinical.fact_claims"},
            {"identifier": "serverless_stable_swv01_catalog.medicaid_clinical.fact_enrollment"},
            {"identifier": "serverless_stable_swv01_catalog.medicaid_clinical.fact_quality_events"}
        ],
        "metric_views": [
            {"identifier": "serverless_stable_swv01_catalog.medicaid_clinical.mv_quality_performance"}
        ]
    },
    "instructions": {
        "text_instructions": [
            {
                "id": "b0000000000000000000000000000001",
                "content": [
                    "This Genie space provides AI-powered analytics for Medicaid clinical quality measures data. It supports HEDIS and CMS Core Set reporting with year-over-year performance comparisons.\n\n",

                    "=== DATA MODEL (Star Schema) ===\n",
                    "DIMENSIONS: dim_member (1000 rows, PK: member_id) - demographics, aid_category, chronic_condition_flags; dim_county (2500 rows, PK: county_fips) - geography; dim_provider (500 rows, PK: provider_npi) - provider registry; dim_measure (18 rows, PK: measure_id) - HEDIS/CMS measure definitions with thresholds.\n",
                    "FACTS: fact_quality_events (10000 rows) - member x measure x year with in_denominator/in_numerator/exclusion_applied flags; fact_enrollment (3000 rows) - monthly snapshots; fact_claims (10000 rows) - claims with ICD-10 dx_codes and CPT proc_codes.\n",
                    "METRIC VIEW: mv_quality_performance - joins fact_quality_events with dim_measure, dim_county, dim_provider, dim_member. Query with MEASURE() function. Measures: denominator, numerator, performance_rate, gap_to_threshold, total_events, exclusion_count, distinct_members, distinct_providers. Dimensions: measure_name, measure_category, measurement_year, quarter, county_name, state_code, region, provider_type, aid_category, gender, race_ethnicity, and more.\n\n",

                    "=== JOIN RELATIONSHIPS ===\n",
                    "fact_quality_events.member_id = dim_member.member_id (quality events to member demographics)\n",
                    "fact_quality_events.measure_id = dim_measure.measure_id (quality events to measure definitions)\n",
                    "fact_quality_events.provider_npi = dim_provider.provider_npi (quality events to provider)\n",
                    "fact_quality_events.county_fips = dim_county.county_fips (quality events to county)\n",
                    "fact_enrollment.member_id = dim_member.member_id (enrollment to member demographics)\n",
                    "fact_enrollment.county_fips = dim_county.county_fips (enrollment to county)\n",
                    "fact_claims.member_id = dim_member.member_id (claims to member demographics)\n",
                    "fact_claims.provider_npi = dim_provider.provider_npi (claims to provider)\n",
                    "dim_member.county_fips = dim_county.county_fips (member residence to county)\n",
                    "dim_provider.county_fips = dim_county.county_fips (provider location to county)\n",
                    "Use mv_quality_performance metric view for quality analytics - it pre-joins all tables.\n\n",

                    "=== JARGON ===\n",
                    "HEDIS: Healthcare Effectiveness Data and Information Set. CMS Core Set: CMS-required Medicaid/CHIP measures. MY: Measurement Year. Performance Rate: (Numerator/Denominator)*100. Regulatory Threshold: min rate required by state contract. At Risk: rate below threshold.\n",
                    "Aid Categories: TANF (low-income families), SSI (aged/blind/disabled), CHIP (children), Expansion Adult (ACA expansion 19-64). Claim Types: IP (inpatient), OP (outpatient), Prof (professional), Rx (pharmacy). Provider Types: PCP, FQHC (community health center), BH (behavioral health). SMI: Serious Mental Illness. MCO: Managed Care Organization. FFS: Fee-For-Service. SUD: Substance Use Disorder. PMPM: Per Member Per Month.\n\n",

                    "=== CALCULATION RULES ===\n",
                    "Performance Rate = COUNT(in_numerator AND NOT exclusion_applied) * 100.0 / NULLIF(COUNT(in_denominator AND NOT exclusion_applied), 0). For Lower is Better measures (CDC-HbA1c, PCR), lower rate = better.\n",
                    "IMPORTANT: mv_quality_performance is a METRIC VIEW. Always wrap measures in MEASURE() function. Do NOT use SELECT *. Always specify dimensions in GROUP BY.\n"
                ]
            }
        ],
        "sql_snippets": {
            "filters": [
                {
                    "id": "f0000000000000000000000000000001",
                    "sql": ["fact_enrollment.is_active = TRUE"],
                    "display_name": "active members only",
                    "synonyms": ["currently enrolled", "active enrollment"],
                    "comment": ["Filters to only actively enrolled members"],
                    "instruction": ["Use when counting current enrollment or active members"]
                },
                {
                    "id": "f0000000000000000000000000000002",
                    "sql": ["fact_quality_events.in_denominator = TRUE AND fact_quality_events.exclusion_applied = FALSE"],
                    "display_name": "eligible for measure",
                    "synonyms": ["in denominator", "eligible population"],
                    "comment": ["Filters to members eligible for a quality measure excluding valid exclusions"],
                    "instruction": ["Use as base filter when calculating quality measure rates"]
                },
                {
                    "id": "f0000000000000000000000000000003",
                    "sql": ["dim_measure.high_priority_flag = TRUE"],
                    "display_name": "high priority measures",
                    "synonyms": ["CMS priority", "key measures", "critical measures"],
                    "comment": ["Filters to CMS-designated high-priority quality measures"],
                    "instruction": ["Use when focusing on the most important regulatory measures"]
                },
                {
                    "id": "f0000000000000000000000000000004",
                    "sql": ["dim_measure.star_rating_flag = TRUE"],
                    "display_name": "star rating measures",
                    "synonyms": ["star measures", "plan rating measures"],
                    "comment": ["Filters to measures included in health plan star ratings"],
                    "instruction": ["Use when analyzing measures impacting plan star ratings"]
                }
            ],
            "expressions": [
                {
                    "id": "f0000000000000000000000000000005",
                    "alias": "measurement_quarter",
                    "sql": ["CONCAT(fact_quality_events.measurement_year, '-Q', fact_quality_events.quarter)"],
                    "display_name": "measurement quarter",
                    "synonyms": ["quarter", "reporting quarter"],
                    "comment": ["Formats measurement year and quarter as YYYY-QN"],
                    "instruction": ["Use for quarter-level trend analysis labels"]
                },
                {
                    "id": "f0000000000000000000000000000006",
                    "alias": "member_age",
                    "sql": ["FLOOR(DATEDIFF(CURRENT_DATE(), dim_member.date_of_birth) / 365.25)"],
                    "display_name": "member age",
                    "synonyms": ["age", "patient age", "enrollee age"],
                    "comment": ["Calculates current age in years from date of birth"],
                    "instruction": ["Use when analyzing by age group or checking age-based eligibility"]
                },
                {
                    "id": "f0000000000000000000000000000007",
                    "alias": "enrollment_month_label",
                    "sql": ["DATE_FORMAT(fact_enrollment.snapshot_month, 'yyyy-MM')"],
                    "display_name": "enrollment month",
                    "synonyms": ["month", "snapshot month"],
                    "comment": ["Formats enrollment snapshot month as YYYY-MM"],
                    "instruction": ["Use for monthly enrollment trend labels"]
                }
            ],
            "measures": [
                {
                    "id": "f0000000000000000000000000000008",
                    "alias": "performance_rate",
                    "sql": ["ROUND(COUNT(CASE WHEN fact_quality_events.in_numerator AND NOT fact_quality_events.exclusion_applied THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN fact_quality_events.in_denominator AND NOT fact_quality_events.exclusion_applied THEN 1 END), 0), 2)"],
                    "display_name": "performance rate",
                    "synonyms": ["compliance rate", "quality rate", "HEDIS rate"],
                    "comment": ["Quality measure performance rate: (numerator/denominator)*100"],
                    "instruction": ["Use for quality measure compliance. For Lower is Better measures (CDC-HbA1c, PCR), lower rate is better."]
                },
                {
                    "id": "f0000000000000000000000000000009",
                    "alias": "total_paid",
                    "sql": ["ROUND(SUM(fact_claims.paid_amount), 2)"],
                    "display_name": "total paid amount",
                    "synonyms": ["total cost", "total spend", "paid claims"],
                    "comment": ["Sum of all claim paid amounts in USD"],
                    "instruction": ["Use for claims cost analysis and financial reporting"]
                },
                {
                    "id": "f000000000000000000000000000000a",
                    "alias": "pmpm_cost",
                    "sql": ["ROUND(SUM(fact_claims.paid_amount) / NULLIF(COUNT(DISTINCT fact_claims.member_id), 0), 2)"],
                    "display_name": "per member cost",
                    "synonyms": ["PMPM", "per member per month", "average cost per member"],
                    "comment": ["Average paid amount per unique member"],
                    "instruction": ["Use for per-member cost analysis and PMPM calculations"]
                }
            ]
        },
        "example_question_sqls": [
            {
                "id": "c0000000000000000000000000000001",
                "question": ["What are our Medicaid enrollment numbers by county?"],
                "sql": ["SELECT c.county_name, c.state_code, COUNT(DISTINCT e.member_id) as enrolled_members, SUM(CASE WHEN e.is_active THEN 1 ELSE 0 END) as active_member_months FROM serverless_stable_swv01_catalog.medicaid_clinical.fact_enrollment e JOIN serverless_stable_swv01_catalog.medicaid_clinical.dim_county c ON e.county_fips = c.county_fips GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 15"]
            },
            {
                "id": "c0000000000000000000000000000002",
                "question": ["Show me clinical quality metrics for the current quarter"],
                "sql": ["SELECT measure_name, measure_category, reporting_direction, regulatory_threshold, MEASURE(denominator) as denominator, MEASURE(numerator) as numerator, MEASURE(performance_rate) as performance_rate, MEASURE(gap_to_threshold) as gap_to_threshold FROM serverless_stable_swv01_catalog.medicaid_clinical.mv_quality_performance WHERE measurement_year = 2025 AND quarter = 1 GROUP BY measure_name, measure_category, reporting_direction, regulatory_threshold ORDER BY measure_name"]
            },
            {
                "id": "c0000000000000000000000000000003",
                "question": ["Which measures are at risk of not meeting regulatory thresholds?"],
                "sql": ["SELECT * FROM (SELECT measure_name, measure_category, measurement_year, reporting_direction, regulatory_threshold, MEASURE(performance_rate) as performance_rate, MEASURE(gap_to_threshold) as gap_to_threshold, MEASURE(denominator) as denominator FROM serverless_stable_swv01_catalog.medicaid_clinical.mv_quality_performance WHERE measurement_year = 2025 GROUP BY measure_name, measure_category, measurement_year, reporting_direction, regulatory_threshold) WHERE (reporting_direction = 'Higher is Better' AND performance_rate < regulatory_threshold) OR (reporting_direction = 'Lower is Better' AND performance_rate > regulatory_threshold) ORDER BY ABS(gap_to_threshold) DESC"]
            },
            {
                "id": "c0000000000000000000000000000004",
                "question": ["Compare this year performance vs last year by quality measure"],
                "sql": ["SELECT measure_name, measure_category, measurement_year, MEASURE(performance_rate) as performance_rate, MEASURE(denominator) as denominator, MEASURE(numerator) as numerator, MEASURE(distinct_members) as distinct_members FROM serverless_stable_swv01_catalog.medicaid_clinical.mv_quality_performance GROUP BY measure_name, measure_category, measurement_year ORDER BY measure_name, measurement_year"]
            },
            {
                "id": "c0000000000000000000000000000005",
                "question": ["Show claims cost breakdown by claim type and aid category"],
                "sql": ["SELECT cl.claim_type, m.aid_category, COUNT(*) as claim_count, ROUND(SUM(cl.paid_amount), 2) as total_paid, ROUND(AVG(cl.paid_amount), 2) as avg_paid, COUNT(DISTINCT cl.member_id) as unique_members FROM serverless_stable_swv01_catalog.medicaid_clinical.fact_claims cl JOIN serverless_stable_swv01_catalog.medicaid_clinical.dim_member m ON cl.member_id = m.member_id GROUP BY 1, 2 ORDER BY 4 DESC"]
            }
        ]
    }
}

# Create the space
payload = {
    "warehouse_id": WAREHOUSE,
    "serialized_space": json.dumps(serialized_space)
}

data = json.dumps(payload).encode()
req = urllib.request.Request(
    f"{HOST}/api/2.0/genie/spaces",
    data=data,
    headers={
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    },
    method="POST"
)
ctx = ssl.create_default_context()
try:
    resp = urllib.request.urlopen(req, context=ctx)
    result = json.loads(resp.read().decode())
    print("Genie space created successfully!")
    print(json.dumps(result, indent=2))
    space_id = result.get("space_id", "")
    print(f"\nGenie Space URL: {HOST}/genie/rooms/{space_id}")
except urllib.error.HTTPError as e:
    body = e.read().decode()
    print(f"HTTP {e.code}: {body}")
