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

                    "=== JOINS ===\n",
                    "fact_quality_events.member_id->dim_member.member_id; fact_quality_events.measure_id->dim_measure.measure_id; fact_quality_events.provider_npi->dim_provider.provider_npi; fact_quality_events.county_fips->dim_county.county_fips; fact_enrollment.member_id->dim_member.member_id; fact_enrollment.county_fips->dim_county.county_fips; fact_claims.member_id->dim_member.member_id; fact_claims.provider_npi->dim_provider.provider_npi; dim_member.county_fips->dim_county.county_fips. Use mv_quality_performance metric view for quality measure analytics - it pre-joins all tables.\n\n",

                    "=== JARGON ===\n",
                    "HEDIS: Healthcare Effectiveness Data and Information Set (NCQA quality measures). CMS Core Set: CMS-required Medicaid/CHIP measures. MY: Measurement Year. Performance Rate: (Numerator/Denominator)*100. Regulatory Threshold: min rate required by state contract. At Risk: rate below threshold.\n",
                    "Aid Categories: TANF (low-income families), SSI (aged/blind/disabled), CHIP (children), Expansion Adult (ACA expansion 19-64). Claim Types: IP (inpatient), OP (outpatient), Prof (professional), Rx (pharmacy). Provider Types: PCP, FQHC (community health center), BH (behavioral health). SMI: Serious Mental Illness. MCO: Managed Care Organization. FFS: Fee-For-Service. SUD: Substance Use Disorder. PMPM: Per Member Per Month.\n\n",

                    "=== CALCULATION RULES ===\n",
                    "Performance Rate = COUNT(in_numerator AND NOT exclusion_applied) * 100.0 / NULLIF(COUNT(in_denominator AND NOT exclusion_applied), 0). For 'Lower is Better' measures (CDC-HbA1c, PCR), lower rate = better. For all others, higher rate = better.\n\n",

                    "=== EXAMPLE QUERIES ===\n",
                    "Enrollment by county: SELECT c.county_name, c.state_code, COUNT(DISTINCT e.member_id) FROM fact_enrollment e JOIN dim_county c USING(county_fips) WHERE e.is_active GROUP BY 1,2 ORDER BY 3 DESC;\n",
                    "Quality metrics by measure and year (METRIC VIEW): SELECT measure_name, measure_category, measurement_year, MEASURE(performance_rate) as rate, MEASURE(denominator) as denom, MEASURE(numerator) as numer, MEASURE(gap_to_threshold) as gap FROM mv_quality_performance GROUP BY measure_name, measure_category, measurement_year ORDER BY measure_name;\n",
                    "At-risk measures: SELECT measure_name, measure_category, measurement_year, regulatory_threshold, MEASURE(performance_rate) as rate, MEASURE(gap_to_threshold) as gap FROM mv_quality_performance WHERE MEASURE(gap_to_threshold) > 0 GROUP BY ALL;\n",
                    "Performance by county: SELECT county_name, state_code, measure_name, MEASURE(performance_rate) as rate FROM mv_quality_performance GROUP BY county_name, state_code, measure_name;\n",
                    "IMPORTANT: mv_quality_performance is a METRIC VIEW. Always wrap measures in MEASURE() function. Do NOT use SELECT *. Always specify dimensions in GROUP BY.\n"
                ]
            }
        ],
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
