-- Drop old standard view
DROP VIEW IF EXISTS serverless_stable_swv01_catalog.medicaid_clinical.v_yoy_quality_performance;

-- Create metric view for quality measure performance
CREATE OR REPLACE VIEW serverless_stable_swv01_catalog.medicaid_clinical.mv_quality_performance
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: >
  Medicaid clinical quality measure performance metric view.
  Computes numerator, denominator, and performance rates across HEDIS and CMS Core Set measures.
  Dimensions include measure, county, year, quarter, provider, and member demographics.
  Use MEASURE() aggregate function when querying measures.

source: serverless_stable_swv01_catalog.medicaid_clinical.fact_quality_events

joins:
  - name: dim_measure
    source: serverless_stable_swv01_catalog.medicaid_clinical.dim_measure
    "on": source.measure_id = dim_measure.measure_id

  - name: dim_county
    source: serverless_stable_swv01_catalog.medicaid_clinical.dim_county
    "on": source.county_fips = dim_county.county_fips

  - name: dim_provider
    source: serverless_stable_swv01_catalog.medicaid_clinical.dim_provider
    "on": source.provider_npi = dim_provider.provider_npi

  - name: dim_member
    source: serverless_stable_swv01_catalog.medicaid_clinical.dim_member
    "on": source.member_id = dim_member.member_id

dimensions:
  - name: measure_id
    expr: source.measure_id
    comment: "NCQA/CMS short code (e.g., CDC-HbA1c, BCS, W34)"

  - name: measure_name
    expr: dim_measure.measure_name
    comment: "Full descriptive name of the quality measure"

  - name: measure_category
    expr: dim_measure.measure_category
    comment: "Clinical domain: Diabetes, Cardiovascular, Preventive, Behavioral Health, etc."

  - name: reporting_standard
    expr: dim_measure.reporting_standard
    comment: "HEDIS, CMS Adult Core Set, or CMS Child Core Set"

  - name: reporting_direction
    expr: dim_measure.reporting_direction
    comment: "Higher is Better or Lower is Better"

  - name: regulatory_threshold
    expr: dim_measure.regulatory_threshold
    comment: "Minimum performance rate required by state Medicaid contract"

  - name: high_priority_flag
    expr: dim_measure.high_priority_flag
    comment: "CMS-designated high-priority measure"

  - name: star_rating_flag
    expr: dim_measure.star_rating_flag
    comment: "Included in health plan star rating calculations"

  - name: measurement_year
    expr: source.measurement_year
    comment: "Measurement year (e.g., 2024, 2025)"

  - name: quarter
    expr: source.quarter
    comment: "Calendar quarter 1-4"

  - name: county_fips
    expr: source.county_fips
    comment: "Five-digit FIPS code of member county at time of event"

  - name: county_name
    expr: dim_county.county_name
    comment: "Human-readable county name"

  - name: state_code
    expr: dim_county.state_code
    comment: "Two-letter US state abbreviation"

  - name: region
    expr: dim_county.region
    comment: "State health region grouping"

  - name: urban_rural_class
    expr: dim_county.urban_rural_class
    comment: "Metropolitan, Micropolitan, Rural, or Frontier"

  - name: provider_npi
    expr: source.provider_npi
    comment: "National Provider Identifier of rendering provider"

  - name: provider_name
    expr: dim_provider.provider_name
    comment: "Provider name"

  - name: provider_type
    expr: dim_provider.provider_type
    comment: "PCP, Specialist, FQHC, BH, OB/GYN, Pediatrics, Urgent Care, Hospital"

  - name: data_source
    expr: source.data_source
    comment: "Admin (claims), EHR, or Hybrid"

  - name: aid_category
    expr: dim_member.aid_category
    comment: "TANF, SSI, CHIP, or Expansion Adult"

  - name: gender
    expr: dim_member.gender
    comment: "Member gender: M, F, Other"

  - name: race_ethnicity
    expr: dim_member.race_ethnicity
    comment: "OMB standard race/ethnicity categories"

measures:
  - name: denominator
    expr: COUNT(CASE WHEN source.in_denominator AND NOT source.exclusion_applied THEN 1 END)
    comment: "Count of eligible members in measure denominator (excluding valid exclusions)"

  - name: numerator
    expr: COUNT(CASE WHEN source.in_numerator AND NOT source.exclusion_applied THEN 1 END)
    comment: "Count of members meeting compliant event criteria (excluding valid exclusions)"

  - name: total_events
    expr: COUNT(*)
    comment: "Total quality event records"

  - name: exclusion_count
    expr: COUNT(CASE WHEN source.exclusion_applied THEN 1 END)
    comment: "Count of events where a valid clinical exclusion was applied"

  - name: performance_rate
    expr: |
      ROUND(
        COUNT(CASE WHEN source.in_numerator AND NOT source.exclusion_applied THEN 1 END) * 100.0
        / NULLIF(COUNT(CASE WHEN source.in_denominator AND NOT source.exclusion_applied THEN 1 END), 0),
        2
      )
    comment: "Quality measure performance rate: (numerator / denominator) * 100. For Higher is Better measures, higher is better. For Lower is Better measures (CDC-HbA1c, PCR), lower is better."

  - name: gap_to_threshold
    expr: |
      ROUND(
        ANY_VALUE(dim_measure.regulatory_threshold) -
        (COUNT(CASE WHEN source.in_numerator AND NOT source.exclusion_applied THEN 1 END) * 100.0
         / NULLIF(COUNT(CASE WHEN source.in_denominator AND NOT source.exclusion_applied THEN 1 END), 0)),
        2
      )
    comment: "Gap between current performance rate and regulatory threshold. Positive means below threshold (at risk for Higher is Better measures)."

  - name: distinct_members
    expr: COUNT(DISTINCT source.member_id)
    comment: "Count of distinct members with quality events"

  - name: distinct_providers
    expr: COUNT(DISTINCT source.provider_npi)
    comment: "Count of distinct rendering providers"
$$;
