"""Validate benchmark queries against the Databricks warehouse."""
import subprocess, json

PROFILE = "fe-vm-fevm-serverless-stable-swv01"
WAREHOUSE = "084543d48aafaeb2"
CATALOG = "serverless_stable_swv01_catalog"
SCHEMA = "medicaid_clinical"

BENCHMARKS = [
    {
        "name": "Q1: Enrollment by county",
        "sql": f"""SELECT c.county_name, c.state_code, COUNT(DISTINCT e.member_id) as enrolled_members,
SUM(CASE WHEN e.is_active THEN 1 ELSE 0 END) as active_member_months
FROM {CATALOG}.{SCHEMA}.fact_enrollment e
JOIN {CATALOG}.{SCHEMA}.dim_county c ON e.county_fips = c.county_fips
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 15"""
    },
    {
        "name": "Q2: Quality metrics for current quarter (metric view)",
        "sql": f"""SELECT measure_name, measure_category, reporting_direction, regulatory_threshold,
MEASURE(denominator) as denominator, MEASURE(numerator) as numerator,
MEASURE(performance_rate) as performance_rate, MEASURE(gap_to_threshold) as gap_to_threshold
FROM {CATALOG}.{SCHEMA}.mv_quality_performance
WHERE measurement_year = 2025 AND quarter = 1
GROUP BY measure_name, measure_category, reporting_direction, regulatory_threshold
ORDER BY measure_name"""
    },
    {
        "name": "Q3: At-risk measures not meeting thresholds",
        "sql": f"""SELECT * FROM (
  SELECT measure_name, measure_category, measurement_year, reporting_direction,
  regulatory_threshold, MEASURE(performance_rate) as performance_rate,
  MEASURE(gap_to_threshold) as gap_to_threshold, MEASURE(denominator) as denominator
  FROM {CATALOG}.{SCHEMA}.mv_quality_performance
  WHERE measurement_year = 2025
  GROUP BY measure_name, measure_category, measurement_year, reporting_direction, regulatory_threshold
) WHERE (reporting_direction = 'Higher is Better' AND performance_rate < regulatory_threshold)
     OR (reporting_direction = 'Lower is Better' AND performance_rate > regulatory_threshold)
ORDER BY ABS(gap_to_threshold) DESC"""
    },
    {
        "name": "Q4: YoY performance comparison by measure",
        "sql": f"""SELECT measure_name, measure_category, measurement_year,
MEASURE(performance_rate) as performance_rate, MEASURE(denominator) as denominator,
MEASURE(numerator) as numerator, MEASURE(distinct_members) as distinct_members
FROM {CATALOG}.{SCHEMA}.mv_quality_performance
GROUP BY measure_name, measure_category, measurement_year
ORDER BY measure_name, measurement_year"""
    },
    {
        "name": "Q5: Claims cost by type and aid category",
        "sql": f"""SELECT cl.claim_type, m.aid_category, COUNT(*) as claim_count,
ROUND(SUM(cl.paid_amount), 2) as total_paid, ROUND(AVG(cl.paid_amount), 2) as avg_paid,
COUNT(DISTINCT cl.member_id) as unique_members
FROM {CATALOG}.{SCHEMA}.fact_claims cl
JOIN {CATALOG}.{SCHEMA}.dim_member m ON cl.member_id = m.member_id
GROUP BY 1, 2 ORDER BY 4 DESC"""
    },
]

for bm in BENCHMARKS:
    print(f"\n=== {bm['name']} ===")
    payload = {"warehouse_id": WAREHOUSE, "statement": bm["sql"], "wait_timeout": "50s"}
    cmd = ["databricks", "api", "post", "/api/2.0/sql/statements", f"--profile={PROFILE}", "--json", json.dumps(payload)]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=80)
    resp = json.loads(result.stdout)
    state = resp.get("status", {}).get("state", "")
    err = resp.get("status", {}).get("error", {}).get("message", "")
    rows = resp.get("result", {}).get("data_array", [])
    if err:
        print(f"ERROR: {err[:300]}")
    else:
        cols = [c["name"] for c in resp.get("manifest", {}).get("schema", {}).get("columns", [])]
        print(f"{state}: {len(rows)} rows | Columns: {', '.join(cols)}")
        if rows:
            print(f"Sample row: {rows[0]}")
