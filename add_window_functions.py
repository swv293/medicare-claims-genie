"""
Add window function benchmark queries and sample questions to the Genie space.
Metric views (YAML) don't support window functions in measure definitions,
so we teach Genie window function patterns via benchmarks and instructions.
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


# ─── New sample questions (window function focused) ───
new_sample_questions = [
    {
        "id": "a0000000000000000000000000000009",
        "question": ["Rank providers by their quality measure performance rate"]
    },
    {
        "id": "a000000000000000000000000000000a",
        "question": ["Show quarter-over-quarter performance trend for each measure"]
    },
    {
        "id": "a000000000000000000000000000000b",
        "question": ["Which providers are in the bottom quartile for quality performance?"]
    },
    {
        "id": "a000000000000000000000000000000c",
        "question": ["Show cumulative enrollment growth with a 3-month rolling average"]
    },
    {
        "id": "a000000000000000000000000000000d",
        "question": ["What percentile does each county rank in for diabetes measure performance?"]
    },
]

# ─── New benchmark queries with window functions ───
new_benchmarks = [
    # RANK() - Provider ranking by measure
    {
        "id": "b0000000000000000000000000000006",
        "question": ["Rank providers by their quality measure performance rate"],
        "answer": [{"format": "SQL", "content": [
            f"SELECT provider_name, provider_type, measure_name, performance_rate, provider_rank "
            f"FROM ("
            f"SELECT p.provider_name, p.provider_type, m.measure_name, "
            f"ROUND(COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 "
            f"/ NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0), 2) as performance_rate, "
            f"RANK() OVER(PARTITION BY m.measure_name ORDER BY "
            f"COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 "
            f"/ NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0) DESC) as provider_rank "
            f"FROM {FQN}.fact_quality_events q "
            f"JOIN {FQN}.dim_provider p ON q.provider_npi = p.provider_npi "
            f"JOIN {FQN}.dim_measure m ON q.measure_id = m.measure_id "
            f"WHERE q.measurement_year = 2025 AND m.reporting_direction = 'Higher is Better' "
            f"GROUP BY p.provider_name, p.provider_type, m.measure_name"
            f") ranked ORDER BY measure_name, provider_rank"
        ]}]
    },
    # LAG() - Quarter-over-quarter trend
    {
        "id": "b0000000000000000000000000000007",
        "question": ["Show quarter-over-quarter performance trend for each measure"],
        "answer": [{"format": "SQL", "content": [
            f"SELECT measure_name, measurement_year, quarter, performance_rate, "
            f"LAG(performance_rate) OVER(PARTITION BY measure_name ORDER BY measurement_year, quarter) as prev_quarter_rate, "
            f"ROUND(performance_rate - LAG(performance_rate) OVER(PARTITION BY measure_name ORDER BY measurement_year, quarter), 2) as qoq_change "
            f"FROM ("
            f"SELECT m.measure_name, q.measurement_year, q.quarter, "
            f"ROUND(COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 "
            f"/ NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0), 2) as performance_rate "
            f"FROM {FQN}.fact_quality_events q "
            f"JOIN {FQN}.dim_measure m ON q.measure_id = m.measure_id "
            f"GROUP BY m.measure_name, q.measurement_year, q.quarter"
            f") rates ORDER BY measure_name, measurement_year, quarter"
        ]}]
    },
    # NTILE() - Provider performance quartiles
    {
        "id": "b0000000000000000000000000000008",
        "question": ["Which providers are in the bottom quartile for quality performance?"],
        "answer": [{"format": "SQL", "content": [
            f"SELECT provider_name, provider_type, overall_performance_rate, performance_quartile "
            f"FROM ("
            f"SELECT p.provider_name, p.provider_type, "
            f"ROUND(COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 "
            f"/ NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0), 2) as overall_performance_rate, "
            f"NTILE(4) OVER(ORDER BY "
            f"COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 "
            f"/ NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0)) as performance_quartile "
            f"FROM {FQN}.fact_quality_events q "
            f"JOIN {FQN}.dim_provider p ON q.provider_npi = p.provider_npi "
            f"WHERE q.measurement_year = 2025 "
            f"GROUP BY p.provider_name, p.provider_type"
            f") ranked WHERE performance_quartile = 1 ORDER BY overall_performance_rate"
        ]}]
    },
    # SUM() OVER + AVG() OVER - Cumulative enrollment with rolling average
    {
        "id": "b0000000000000000000000000000009",
        "question": ["Show cumulative enrollment growth with a 3-month rolling average"],
        "answer": [{"format": "SQL", "content": [
            f"SELECT snapshot_month, monthly_active_members, "
            f"SUM(monthly_active_members) OVER(ORDER BY snapshot_month) as cumulative_member_months, "
            f"ROUND(AVG(monthly_active_members) OVER(ORDER BY snapshot_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 0) as rolling_3mo_avg "
            f"FROM ("
            f"SELECT e.snapshot_month, COUNT(DISTINCT e.member_id) as monthly_active_members "
            f"FROM {FQN}.fact_enrollment e "
            f"WHERE e.is_active = TRUE "
            f"GROUP BY e.snapshot_month"
            f") monthly ORDER BY snapshot_month"
        ]}]
    },
    # PERCENT_RANK() - County performance percentiles
    {
        "id": "b000000000000000000000000000000a",
        "question": ["What percentile does each county rank in for diabetes measure performance?"],
        "answer": [{"format": "SQL", "content": [
            f"SELECT county_name, state_code, measure_name, performance_rate, "
            f"ROUND(PERCENT_RANK() OVER(PARTITION BY measure_name ORDER BY performance_rate) * 100, 1) as percentile_rank "
            f"FROM ("
            f"SELECT c.county_name, c.state_code, m.measure_name, "
            f"ROUND(COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 "
            f"/ NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0), 2) as performance_rate "
            f"FROM {FQN}.fact_quality_events q "
            f"JOIN {FQN}.dim_county c ON q.county_fips = c.county_fips "
            f"JOIN {FQN}.dim_measure m ON q.measure_id = m.measure_id "
            f"WHERE q.measurement_year = 2025 AND m.measure_category = 'Diabetes' "
            f"GROUP BY c.county_name, c.state_code, m.measure_name"
            f") county_rates ORDER BY measure_name, percentile_rank DESC"
        ]}]
    },
]

# ─── Window function instruction text to append to existing instruction ───
window_instruction_content = [
    "\n\n=== WINDOW FUNCTION PATTERNS ===\n",
    "Use window functions for ranking, trending, and comparative analytics on the base tables (NOT the metric view). The metric view uses MEASURE() aggregates only.\n\n",

    "RANK/DENSE_RANK: Rank providers or counties by performance rate within each measure. Use RANK() OVER(PARTITION BY measure_name ORDER BY performance_rate DESC). DENSE_RANK avoids gaps in ranking.\n",
    "LAG/LEAD: Show quarter-over-quarter or year-over-year changes. Use LAG(performance_rate) OVER(PARTITION BY measure_name ORDER BY measurement_year, quarter) to get previous period rate, then subtract for delta.\n",
    "NTILE: Classify providers or counties into quartiles/deciles. Use NTILE(4) OVER(ORDER BY performance_rate) for quartiles. Quartile 1 = bottom 25% (lowest performers).\n",
    "RUNNING SUM/AVG: Cumulative enrollment or rolling averages. Use SUM(x) OVER(ORDER BY snapshot_month) for cumulative. Use AVG(x) OVER(ORDER BY snapshot_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) for 3-month rolling average.\n",
    "PERCENT_RANK: Percentile ranking of counties or providers. Use ROUND(PERCENT_RANK() OVER(PARTITION BY measure_name ORDER BY performance_rate) * 100, 1) to get 0-100 percentile.\n",
    "ROW_NUMBER: Top-N analysis. Use ROW_NUMBER() OVER(PARTITION BY measure_name ORDER BY performance_rate DESC) then filter WHERE rn <= N.\n\n",

    "IMPORTANT: Window functions require a subquery pattern. First aggregate (GROUP BY) in an inner query, then apply window functions in the outer query. Always compute performance_rate = COUNT(CASE WHEN in_numerator AND NOT exclusion_applied THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN in_denominator AND NOT exclusion_applied THEN 1 END), 0) in the inner query, then rank/trend/partition in the outer query.\n"
]


# ─── Apply updates ───
print("Fetching current Genie space...")
ss = get_space()

# Add new sample questions (keep existing)
existing_questions = ss.get("config", {}).get("sample_questions", [])
existing_ids = {q["id"] for q in existing_questions}
for q in new_sample_questions:
    if q["id"] not in existing_ids:
        existing_questions.append(q)
ss["config"]["sample_questions"] = existing_questions
print(f"  Sample questions: {len(existing_questions)}")

# Add new benchmark queries (keep existing)
existing_benchmarks = ss.get("benchmarks", {}).get("questions", [])
existing_bm_ids = {b["id"] for b in existing_benchmarks}
for b in new_benchmarks:
    if b["id"] not in existing_bm_ids:
        existing_benchmarks.append(b)
ss["benchmarks"]["questions"] = existing_benchmarks
print(f"  Benchmark queries: {len(existing_benchmarks)}")

# Append window function instructions to the single existing text_instructions item
existing_instructions = ss.get("instructions", {}).get("text_instructions", [])
if existing_instructions:
    current_content = existing_instructions[0].get("content", [])
    # Check if window function instructions already exist
    if not any("WINDOW FUNCTION PATTERNS" in c for c in current_content):
        current_content.extend(window_instruction_content)
        existing_instructions[0]["content"] = current_content
        print(f"  Appended window function instructions to existing text block")
    else:
        print(f"  Window function instructions already present")
ss["instructions"]["text_instructions"] = existing_instructions
print(f"  Text instructions: {len(existing_instructions)} (single block required by API)")

# Push update
print("\nUpdating Genie space...")
ok, result = update_space(ss)
if ok:
    print(f"Success: {result.get('title', '')}")
    time.sleep(1)
    ss2 = get_space()
    inst = ss2.get("instructions", {})
    bm = ss2.get("benchmarks", {})
    config = ss2.get("config", {})
    print(f"\nVerification:")
    print(f"  sample_questions: {len(config.get('sample_questions', []))}")
    print(f"  text_instructions: {len(inst.get('text_instructions', []))}")
    snips = inst.get("sql_snippets", {})
    print(f"  sql_snippets.filters: {len(snips.get('filters', []))}")
    print(f"  sql_snippets.expressions: {len(snips.get('expressions', []))}")
    print(f"  sql_snippets.measures: {len(snips.get('measures', []))}")
    print(f"  benchmarks.questions: {len(bm.get('questions', []))}")

    # Print the new questions
    print(f"\n--- New window function questions ---")
    for q in new_sample_questions:
        print(f"  • {q['question'][0]}")
else:
    msg = result.get("message", "")
    print(f"FAILED: {msg[:500]}")
