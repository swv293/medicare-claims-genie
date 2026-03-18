"""
Update Genie space with structured join_specs, sql_snippets, and benchmarks.
Moves joins and SQL examples from text_instructions into their dedicated tabs.
"""
import json
import urllib.request
import ssl
import subprocess
import time

PROFILE = "fe-vm-fevm-serverless-stable-swv01"
SPACE_ID = "01f1226919431c51b114987846f1a776"
WAREHOUSE = "084543d48aafaeb2"
CATALOG = "serverless_stable_swv01_catalog"
SCHEMA = "medicaid_clinical"
FQN = f"{CATALOG}.{SCHEMA}"

env_out = subprocess.run(["databricks", "auth", "env", f"--profile={PROFILE}"], capture_output=True, text=True)
HOST = json.loads(env_out.stdout)["env"]["DATABRICKS_HOST"]
token_out = subprocess.run(["databricks", "auth", "token", f"--profile={PROFILE}"], capture_output=True, text=True)
TOKEN = json.loads(token_out.stdout)["access_token"]
ctx = ssl.create_default_context()


def get_space():
    req = urllib.request.Request(
        f"{HOST}/api/2.0/genie/spaces/{SPACE_ID}?include_serialized_space=true",
        headers={"Authorization": f"Bearer {TOKEN}"}
    )
    resp = urllib.request.urlopen(req, context=ctx)
    d = json.loads(resp.read().decode())
    return json.loads(d["serialized_space"])


def update_space(ss):
    payload = {"warehouse_id": WAREHOUSE, "serialized_space": json.dumps(ss)}
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{HOST}/api/2.0/genie/spaces/{SPACE_ID}",
        data=data,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        method="PATCH"
    )
    try:
        resp = urllib.request.urlopen(req, context=ctx)
        result = json.loads(resp.read().decode())
        return True, result
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        return False, json.loads(body)


# ─── Join Specs (sorted by id) ───
join_specs = [
    {
        "id": "e0000000000000000000000000000001",
        "left": {"identifier": f"{FQN}.dim_member", "alias": "dim_member"},
        "right": {"identifier": f"{FQN}.dim_county", "alias": "dim_county"},
        "sql": ["dim_member.county_fips = dim_county.county_fips"],
        "comment": ["Join member to county for geographic analysis of member residence"],
        "instruction": ["Use when you need member county name, state, region, or urban/rural classification"]
    },
    {
        "id": "e0000000000000000000000000000002",
        "left": {"identifier": f"{FQN}.dim_provider", "alias": "dim_provider"},
        "right": {"identifier": f"{FQN}.dim_county", "alias": "dim_county"},
        "sql": ["dim_provider.county_fips = dim_county.county_fips"],
        "comment": ["Join provider to county for provider practice location analysis"],
        "instruction": ["Use when you need provider location details"]
    },
    {
        "id": "e0000000000000000000000000000003",
        "left": {"identifier": f"{FQN}.fact_claims", "alias": "fact_claims"},
        "right": {"identifier": f"{FQN}.dim_member", "alias": "dim_member"},
        "sql": ["fact_claims.member_id = dim_member.member_id"],
        "comment": ["Join claims to member demographics"],
        "instruction": ["Use when analyzing claims by member demographics like aid category, gender, or chronic conditions"]
    },
    {
        "id": "e0000000000000000000000000000004",
        "left": {"identifier": f"{FQN}.fact_claims", "alias": "fact_claims"},
        "right": {"identifier": f"{FQN}.dim_provider", "alias": "dim_provider"},
        "sql": ["fact_claims.provider_npi = dim_provider.provider_npi"],
        "comment": ["Join claims to provider for provider-level claims analysis"],
        "instruction": ["Use when analyzing claims by provider type or specific providers"]
    },
    {
        "id": "e0000000000000000000000000000005",
        "left": {"identifier": f"{FQN}.fact_enrollment", "alias": "fact_enrollment"},
        "right": {"identifier": f"{FQN}.dim_county", "alias": "dim_county"},
        "sql": ["fact_enrollment.county_fips = dim_county.county_fips"],
        "comment": ["Join enrollment to county for enrollment by geography"],
        "instruction": ["Use when analyzing enrollment numbers by county, state, or region"]
    },
    {
        "id": "e0000000000000000000000000000006",
        "left": {"identifier": f"{FQN}.fact_enrollment", "alias": "fact_enrollment"},
        "right": {"identifier": f"{FQN}.dim_member", "alias": "dim_member"},
        "sql": ["fact_enrollment.member_id = dim_member.member_id"],
        "comment": ["Join enrollment to member demographics"],
        "instruction": ["Use when analyzing enrollment trends by member demographics"]
    },
    {
        "id": "e0000000000000000000000000000007",
        "left": {"identifier": f"{FQN}.fact_quality_events", "alias": "fact_quality_events"},
        "right": {"identifier": f"{FQN}.dim_county", "alias": "dim_county"},
        "sql": ["fact_quality_events.county_fips = dim_county.county_fips"],
        "comment": ["Join quality events to county for geographic quality analysis"],
        "instruction": ["Use when analyzing quality measure performance by county or region"]
    },
    {
        "id": "e0000000000000000000000000000008",
        "left": {"identifier": f"{FQN}.fact_quality_events", "alias": "fact_quality_events"},
        "right": {"identifier": f"{FQN}.dim_measure", "alias": "dim_measure"},
        "sql": ["fact_quality_events.measure_id = dim_measure.measure_id"],
        "comment": ["Join quality events to measure definitions for measure metadata"],
        "instruction": ["Use when you need measure name, category, thresholds, or reporting direction alongside quality event data"]
    },
    {
        "id": "e0000000000000000000000000000009",
        "left": {"identifier": f"{FQN}.fact_quality_events", "alias": "fact_quality_events"},
        "right": {"identifier": f"{FQN}.dim_member", "alias": "dim_member"},
        "sql": ["fact_quality_events.member_id = dim_member.member_id"],
        "comment": ["Join quality events to member demographics"],
        "instruction": ["Use when analyzing quality measures by member demographics, aid category, or chronic conditions"]
    },
    {
        "id": "e000000000000000000000000000000a",
        "left": {"identifier": f"{FQN}.fact_quality_events", "alias": "fact_quality_events"},
        "right": {"identifier": f"{FQN}.dim_provider", "alias": "dim_provider"},
        "sql": ["fact_quality_events.provider_npi = dim_provider.provider_npi"],
        "comment": ["Join quality events to provider for provider-level quality analysis"],
        "instruction": ["Use when analyzing quality measure compliance by provider type or specific providers"]
    },
]

# ─── SQL Snippets ───
sql_snippets = {
    "filters": [
        {
            "id": "f0000000000000000000000000000001",
            "sql": [f"fact_enrollment.is_active = TRUE"],
            "display_name": "active members only",
            "synonyms": ["currently enrolled", "active enrollment"],
            "comment": ["Filters to only actively enrolled members"],
            "instruction": ["Use when counting current enrollment or active members"]
        },
        {
            "id": "f0000000000000000000000000000002",
            "sql": [f"fact_quality_events.in_denominator = TRUE AND fact_quality_events.exclusion_applied = FALSE"],
            "display_name": "eligible for measure",
            "synonyms": ["in denominator", "eligible population", "measure eligible"],
            "comment": ["Filters to members eligible for a quality measure (in denominator, no exclusion)"],
            "instruction": ["Use as base filter when calculating quality measure rates"]
        },
        {
            "id": "f0000000000000000000000000000003",
            "sql": [f"dim_measure.high_priority_flag = TRUE"],
            "display_name": "high priority measures",
            "synonyms": ["CMS priority", "key measures", "critical measures"],
            "comment": ["Filters to CMS-designated high-priority quality measures"],
            "instruction": ["Use when focusing on the most important measures for regulatory reporting"]
        },
        {
            "id": "f0000000000000000000000000000004",
            "sql": [f"dim_measure.star_rating_flag = TRUE"],
            "display_name": "star rating measures",
            "synonyms": ["star measures", "plan rating measures"],
            "comment": ["Filters to measures included in health plan star ratings"],
            "instruction": ["Use when analyzing measures that impact plan star ratings"]
        },
    ],
    "expressions": [
        {
            "id": "f0000000000000000000000000000005",
            "alias": "measurement_quarter",
            "sql": [f"CONCAT(fact_quality_events.measurement_year, '-Q', fact_quality_events.quarter)"],
            "display_name": "measurement quarter",
            "synonyms": ["quarter", "reporting quarter", "MY quarter"],
            "comment": ["Formats measurement year and quarter as YYYY-QN"],
            "instruction": ["Use for quarter-level trend analysis labels"]
        },
        {
            "id": "f0000000000000000000000000000006",
            "alias": "member_age",
            "sql": [f"FLOOR(DATEDIFF(CURRENT_DATE(), dim_member.date_of_birth) / 365.25)"],
            "display_name": "member age",
            "synonyms": ["age", "patient age", "enrollee age"],
            "comment": ["Calculates current age in years from date of birth"],
            "instruction": ["Use when analyzing by age group or checking age-based eligibility"]
        },
        {
            "id": "f0000000000000000000000000000007",
            "alias": "enrollment_month_label",
            "sql": [f"DATE_FORMAT(fact_enrollment.snapshot_month, 'yyyy-MM')"],
            "display_name": "enrollment month",
            "synonyms": ["month", "snapshot month", "enrollment period"],
            "comment": ["Formats enrollment snapshot month as YYYY-MM"],
            "instruction": ["Use for monthly enrollment trend labels"]
        },
    ],
    "measures": [
        {
            "id": "f0000000000000000000000000000008",
            "alias": "performance_rate",
            "sql": [f"ROUND(COUNT(CASE WHEN fact_quality_events.in_numerator AND NOT fact_quality_events.exclusion_applied THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN fact_quality_events.in_denominator AND NOT fact_quality_events.exclusion_applied THEN 1 END), 0), 2)"],
            "display_name": "performance rate",
            "synonyms": ["compliance rate", "quality rate", "measure rate", "HEDIS rate"],
            "comment": ["Quality measure performance rate: (numerator / denominator) * 100. Excludes members with valid exclusions."],
            "instruction": ["Use for calculating quality measure compliance rates. For Lower is Better measures (CDC-HbA1c, PCR), a lower rate is better."]
        },
        {
            "id": "f0000000000000000000000000000009",
            "alias": "total_paid",
            "sql": [f"ROUND(SUM(fact_claims.paid_amount), 2)"],
            "display_name": "total paid amount",
            "synonyms": ["total cost", "total spend", "paid claims"],
            "comment": ["Sum of all claim paid amounts in USD"],
            "instruction": ["Use for claims cost analysis and financial reporting"]
        },
        {
            "id": "f000000000000000000000000000000a",
            "alias": "pmpm_cost",
            "sql": [f"ROUND(SUM(fact_claims.paid_amount) / NULLIF(COUNT(DISTINCT fact_claims.member_id), 0), 2)"],
            "display_name": "per member cost",
            "synonyms": ["PMPM", "per member per month", "average cost per member"],
            "comment": ["Average paid amount per unique member"],
            "instruction": ["Use for per-member cost analysis and PMPM calculations"]
        },
    ]
}

# ─── Benchmarks ───
benchmarks = {
    "questions": [
        {
            "id": "b0000000000000000000000000000001",
            "question": ["What are our Medicaid enrollment numbers by county?"],
            "answer": [{"format": "SQL", "content": [f"SELECT c.county_name, c.state_code, COUNT(DISTINCT e.member_id) as enrolled_members, SUM(CASE WHEN e.is_active THEN 1 ELSE 0 END) as active_member_months FROM {FQN}.fact_enrollment e JOIN {FQN}.dim_county c ON e.county_fips = c.county_fips GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 15"]}]
        },
        {
            "id": "b0000000000000000000000000000002",
            "question": ["Show me clinical quality metrics for the current quarter"],
            "answer": [{"format": "SQL", "content": [f"SELECT measure_name, measure_category, reporting_direction, regulatory_threshold, MEASURE(denominator) as denominator, MEASURE(numerator) as numerator, MEASURE(performance_rate) as performance_rate, MEASURE(gap_to_threshold) as gap_to_threshold FROM {FQN}.mv_quality_performance WHERE measurement_year = 2025 AND quarter = 1 GROUP BY measure_name, measure_category, reporting_direction, regulatory_threshold ORDER BY measure_name"]}]
        },
        {
            "id": "b0000000000000000000000000000003",
            "question": ["Which measures are at risk of not meeting regulatory thresholds?"],
            "answer": [{"format": "SQL", "content": [f"SELECT * FROM (SELECT measure_name, measure_category, measurement_year, reporting_direction, regulatory_threshold, MEASURE(performance_rate) as performance_rate, MEASURE(gap_to_threshold) as gap_to_threshold, MEASURE(denominator) as denominator FROM {FQN}.mv_quality_performance WHERE measurement_year = 2025 GROUP BY measure_name, measure_category, measurement_year, reporting_direction, regulatory_threshold) WHERE (reporting_direction = 'Higher is Better' AND performance_rate < regulatory_threshold) OR (reporting_direction = 'Lower is Better' AND performance_rate > regulatory_threshold) ORDER BY ABS(gap_to_threshold) DESC"]}]
        },
        {
            "id": "b0000000000000000000000000000004",
            "question": ["Compare this year performance vs last year by quality measure"],
            "answer": [{"format": "SQL", "content": [f"SELECT measure_name, measure_category, measurement_year, MEASURE(performance_rate) as performance_rate, MEASURE(denominator) as denominator, MEASURE(numerator) as numerator, MEASURE(distinct_members) as distinct_members FROM {FQN}.mv_quality_performance GROUP BY measure_name, measure_category, measurement_year ORDER BY measure_name, measurement_year"]}]
        },
        {
            "id": "b0000000000000000000000000000005",
            "question": ["Show claims cost breakdown by claim type and aid category"],
            "answer": [{"format": "SQL", "content": [f"SELECT cl.claim_type, m.aid_category, COUNT(*) as claim_count, ROUND(SUM(cl.paid_amount), 2) as total_paid, ROUND(AVG(cl.paid_amount), 2) as avg_paid, COUNT(DISTINCT cl.member_id) as unique_members FROM {FQN}.fact_claims cl JOIN {FQN}.dim_member m ON cl.member_id = m.member_id GROUP BY 1, 2 ORDER BY 4 DESC"]}]
        },
    ]
}

# ─── Updated text instructions (joins and SQL examples removed) ───
text_instructions = [
    {
        "id": "b0000000000000000000000000000010",
        "content": [
            "This Genie space provides AI-powered analytics for Medicaid clinical quality measures data. It supports HEDIS and CMS Core Set reporting with year-over-year performance comparisons.\n\n",

            "=== DATA MODEL (Star Schema) ===\n",
            "DIMENSIONS: dim_member (1000 rows, PK: member_id) - demographics, aid_category, chronic_condition_flags; dim_county (2500 rows, PK: county_fips) - geography; dim_provider (500 rows, PK: provider_npi) - provider registry; dim_measure (18 rows, PK: measure_id) - HEDIS/CMS measure definitions with thresholds.\n",
            "FACTS: fact_quality_events (10000 rows) - member x measure x year with in_denominator/in_numerator/exclusion_applied flags; fact_enrollment (3000 rows) - monthly snapshots; fact_claims (10000 rows) - claims with ICD-10 dx_codes and CPT proc_codes.\n",
            "METRIC VIEW: mv_quality_performance - joins fact_quality_events with dim_measure, dim_county, dim_provider, dim_member. Query with MEASURE() function. Measures: denominator, numerator, performance_rate, gap_to_threshold, total_events, exclusion_count, distinct_members, distinct_providers. Dimensions: measure_name, measure_category, measurement_year, quarter, county_name, state_code, region, provider_type, aid_category, gender, race_ethnicity, and more.\n\n",

            "=== JARGON ===\n",
            "HEDIS: Healthcare Effectiveness Data and Information Set (NCQA quality measures). CMS Core Set: CMS-required Medicaid/CHIP measures. MY: Measurement Year. Performance Rate: (Numerator/Denominator)*100. Regulatory Threshold: min rate required by state contract. At Risk: rate below threshold.\n",
            "Aid Categories: TANF (low-income families), SSI (aged/blind/disabled), CHIP (children), Expansion Adult (ACA expansion 19-64). Claim Types: IP (inpatient), OP (outpatient), Prof (professional), Rx (pharmacy). Provider Types: PCP, FQHC (community health center), BH (behavioral health). SMI: Serious Mental Illness. MCO: Managed Care Organization. FFS: Fee-For-Service. SUD: Substance Use Disorder. PMPM: Per Member Per Month.\n\n",

            "=== CALCULATION RULES ===\n",
            "Performance Rate = COUNT(in_numerator AND NOT exclusion_applied) * 100.0 / NULLIF(COUNT(in_denominator AND NOT exclusion_applied), 0). For 'Lower is Better' measures (CDC-HbA1c, PCR), lower rate = better. For all others, higher rate = better.\n",
            "IMPORTANT: mv_quality_performance is a METRIC VIEW. Always wrap measures in MEASURE() function. Do NOT use SELECT *. Always specify dimensions in GROUP BY.\n"
        ]
    }
]


# ─── Apply update ───
print("Fetching current space...")
ss = get_space()

# Update instructions - remove old, add structured
ss["instructions"] = {
    "text_instructions": text_instructions,
    "example_question_sqls": ss["instructions"].get("example_question_sqls", []),
    "sql_snippets": sql_snippets,
}

# Try join_specs and benchmarks in instructions
ss["instructions"]["join_specs"] = join_specs
# Note: benchmarks are managed via the Genie UI, not the serialized_space export API

print("Updating space...")
ok, result = update_space(ss)
if ok:
    print(f"Success: {result.get('title', '')}")

    # Verify
    time.sleep(1)
    ss2 = get_space()
    inst = ss2.get("instructions", {})
    print(f"\nVerification:")
    print(f"  text_instructions: {len(inst.get('text_instructions', []))}")
    print(f"  example_question_sqls: {len(inst.get('example_question_sqls', []))}")
    snips = inst.get("sql_snippets", {})
    print(f"  sql_snippets.filters: {len(snips.get('filters', []))}")
    print(f"  sql_snippets.expressions: {len(snips.get('expressions', []))}")
    print(f"  sql_snippets.measures: {len(snips.get('measures', []))}")
    print(f"  join_specs: {len(ss2.get('join_specs', []))}")
    bm = ss2.get("benchmarks", {})
    print(f"  benchmarks.questions: {len(bm.get('questions', []))}")
else:
    msg = result.get("message", "")
    print(f"FAILED: {msg[:500]}")
