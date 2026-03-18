-- ============================================================
-- Medicaid Clinical Quality Measures Data Warehouse
-- DDL for Delta Tables with descriptions and tags
-- ============================================================

-- 1. dim_county
CREATE OR REPLACE TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_county (
  county_fips STRING NOT NULL COMMENT 'Five-digit FIPS code uniquely identifying the county',
  county_name STRING NOT NULL COMMENT 'Human-readable county name',
  region STRING COMMENT 'State health region grouping for geographic analysis',
  urban_rural_class STRING COMMENT 'Rural-Urban Continuum Code (RUCC) classification: Metropolitan, Micropolitan, Rural, Frontier',
  state_code STRING NOT NULL COMMENT 'Two-letter US state abbreviation'
)
USING DELTA
COMMENT 'County reference dimension containing geographic and classification data for all US counties used in Medicaid reporting. Links to member and provider locations.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality' = 'gold'
);

ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_county SET TAGS ('domain' = 'reference', 'data_classification' = 'public', 'contains_phi' = 'false', 'contains_pii' = 'false');

-- 2. dim_provider
CREATE OR REPLACE TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_provider (
  provider_npi STRING NOT NULL COMMENT 'National Provider Identifier (NPI) - unique 10-digit provider ID from CMS NPPES',
  provider_name STRING NOT NULL COMMENT 'Individual or organizational provider name',
  provider_type STRING NOT NULL COMMENT 'Provider classification: PCP, Specialist, FQHC, BH (Behavioral Health), OB/GYN, Pediatrics, Urgent Care, Hospital',
  specialty_code STRING COMMENT 'NUCC Healthcare Provider Taxonomy Code classifying provider specialty',
  county_fips STRING COMMENT 'FIPS code of the provider primary practice location county',
  accepting_medicaid BOOLEAN COMMENT 'Whether the provider is currently accepting new Medicaid patients'
)
USING DELTA
COMMENT 'Provider registry dimension containing demographics and practice details for all providers serving Medicaid members. Used to attribute clinical events and track network adequacy.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality' = 'gold'
);

ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_provider SET TAGS ('domain' = 'provider', 'data_classification' = 'confidential', 'contains_phi' = 'false', 'contains_pii' = 'true');

-- 3. dim_member
CREATE OR REPLACE TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member (
  member_id STRING NOT NULL COMMENT 'Unique Medicaid member identifier assigned by the state Medicaid agency',
  date_of_birth DATE NOT NULL COMMENT 'Member date of birth, used for age-based eligibility determination',
  gender STRING COMMENT 'Member gender: M (Male), F (Female), Other',
  race_ethnicity STRING COMMENT 'Self-reported race/ethnicity per OMB standard categories for health equity analysis',
  county_fips STRING COMMENT 'FIPS code of member residence county, links to dim_county',
  zip_code STRING COMMENT 'Five-digit ZIP code of member residence',
  aid_category STRING NOT NULL COMMENT 'Medicaid eligibility category: TANF (Temporary Assistance), SSI (Supplemental Security Income), CHIP (Children Health Insurance), Expansion Adult',
  smi_flag BOOLEAN COMMENT 'Serious Mental Illness (SMI) designation flag - TRUE if member has qualifying SMI diagnosis',
  chronic_condition_flags ARRAY<STRING> COMMENT 'Array of chronic condition flags: Diabetes, Hypertension, Asthma, COPD, CHF, Depression, Anxiety, Obesity, CKD, Substance Use Disorder',
  enrollment_start_dt DATE NOT NULL COMMENT 'Medicaid eligibility enrollment start date',
  enrollment_end_dt DATE COMMENT 'Medicaid eligibility enrollment end date. NULL indicates currently active enrollment'
)
USING DELTA
COMMENT 'Member demographics dimension containing one row per Medicaid enrollee. Core dimension joining to all clinical events, enrollment snapshots, and claims. Contains PHI and PII data requiring HIPAA-compliant access controls.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality' = 'gold'
);

ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member SET TAGS ('domain' = 'member', 'data_classification' = 'restricted', 'contains_phi' = 'true', 'contains_pii' = 'true', 'hipaa' = 'true');

-- Tag individual PHI/PII columns on dim_member
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN member_id SET TAGS ('pii' = 'true', 'phi' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN date_of_birth SET TAGS ('pii' = 'true', 'phi' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN gender SET TAGS ('pii' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN race_ethnicity SET TAGS ('pii' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN zip_code SET TAGS ('pii' = 'true', 'phi' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN chronic_condition_flags SET TAGS ('phi' = 'true');

-- 4. dim_measure
CREATE OR REPLACE TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_measure (
  measure_id STRING NOT NULL COMMENT 'NCQA/CMS short code uniquely identifying the quality measure (e.g., CDC-HbA1c, BCS, W34)',
  measure_name STRING NOT NULL COMMENT 'Full descriptive name of the quality measure',
  measure_category STRING NOT NULL COMMENT 'Clinical domain grouping: Diabetes, Cardiovascular, Preventive, Child Health, Immunization, Behavioral Health, Respiratory, Maternal Health, Nutrition/Obesity, Utilization',
  reporting_standard STRING NOT NULL COMMENT 'Quality reporting framework: HEDIS, CMS Adult Core Set, CMS Child Core Set',
  numerator_definition STRING COMMENT 'Clinical criteria defining what counts as a compliant event for rate calculation',
  denominator_definition STRING COMMENT 'Population eligibility criteria defining who is included in the rate calculation',
  exclusion_definition STRING COMMENT 'Valid clinical exclusions that remove members from the denominator',
  age_range STRING COMMENT 'Age range of eligible population (e.g., 18-75, 2-21)',
  measurement_year INT NOT NULL COMMENT 'Measurement year this specification applies to',
  reporting_direction STRING NOT NULL COMMENT 'Whether higher or lower rates indicate better performance: Higher is Better / Lower is Better',
  regulatory_threshold DECIMAL(5,2) COMMENT 'Minimum performance percentage required by state Medicaid contract',
  high_priority_flag BOOLEAN COMMENT 'CMS-designated high-priority measure requiring enhanced monitoring',
  star_rating_flag BOOLEAN COMMENT 'Whether this measure is included in health plan star rating calculations',
  data_source STRING COMMENT 'Data collection methodology: Admin (claims only), Hybrid (claims + chart review), ECDS (electronic clinical data)'
)
USING DELTA
COMMENT 'Quality measure definitions dimension containing HEDIS MY 2025 and CMS 2025 Adult/Child Core Set specifications. Each row defines one clinical quality measure with numerator, denominator, exclusion criteria, and regulatory thresholds.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality' = 'gold'
);

ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_measure SET TAGS ('domain' = 'quality_measures', 'data_classification' = 'internal', 'contains_phi' = 'false', 'contains_pii' = 'false');

-- 5. fact_quality_events
CREATE OR REPLACE TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_quality_events (
  event_id BIGINT NOT NULL COMMENT 'Surrogate key uniquely identifying each quality measure event',
  member_id STRING NOT NULL COMMENT 'Member identifier linking to dim_member',
  measure_id STRING NOT NULL COMMENT 'Quality measure identifier linking to dim_measure',
  measurement_year INT NOT NULL COMMENT 'Measurement year for this quality event (e.g., 2024, 2025)',
  quarter INT NOT NULL COMMENT 'Calendar quarter (1-4) when the qualifying event occurred',
  in_denominator BOOLEAN NOT NULL COMMENT 'TRUE if the member met eligibility criteria for the measure denominator',
  in_numerator BOOLEAN NOT NULL COMMENT 'TRUE if the member met the compliant clinical event criteria for the numerator',
  exclusion_applied BOOLEAN COMMENT 'TRUE if a valid clinical exclusion was applied, removing member from performance calculation',
  service_date DATE COMMENT 'Date of the qualifying clinical service or event',
  provider_npi STRING COMMENT 'NPI of the rendering provider who performed the service, links to dim_provider',
  data_source STRING COMMENT 'Source of the quality event data: Admin (claims), EHR (electronic health record), Hybrid',
  county_fips STRING COMMENT 'FIPS code of the member county at the time of the event, links to dim_county'
)
USING DELTA
COMMENT 'Primary fact table for clinical quality measure events. One row per member x measure x measurement year. Drives all HEDIS and CMS Core Set performance rate calculations. Join to dim_measure for thresholds and dim_member for demographics.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality' = 'gold'
);

ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_quality_events SET TAGS ('domain' = 'quality_measures', 'data_classification' = 'restricted', 'contains_phi' = 'true', 'contains_pii' = 'true', 'hipaa' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_quality_events ALTER COLUMN member_id SET TAGS ('pii' = 'true', 'phi' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_quality_events ALTER COLUMN service_date SET TAGS ('phi' = 'true');

-- 6. fact_enrollment
CREATE OR REPLACE TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_enrollment (
  snapshot_month DATE NOT NULL COMMENT 'First day of the month for this enrollment snapshot',
  member_id STRING NOT NULL COMMENT 'Member identifier linking to dim_member',
  aid_category STRING NOT NULL COMMENT 'Medicaid enrollment category at time of snapshot: TANF, SSI, CHIP, Expansion Adult',
  county_fips STRING COMMENT 'FIPS code of member county at time of snapshot, links to dim_county',
  plan_id STRING COMMENT 'Managed Care Organization (MCO) plan ID or FFS (Fee-For-Service) designation',
  is_active BOOLEAN NOT NULL COMMENT 'TRUE if member was actively enrolled on this snapshot month'
)
USING DELTA
COMMENT 'Monthly enrollment snapshot fact table. One row per member per month. Used for continuous enrollment logic in denominator calculations and enrollment trend analysis by county, aid category, and plan.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality' = 'gold'
);

ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_enrollment SET TAGS ('domain' = 'enrollment', 'data_classification' = 'restricted', 'contains_phi' = 'true', 'contains_pii' = 'true', 'hipaa' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_enrollment ALTER COLUMN member_id SET TAGS ('pii' = 'true', 'phi' = 'true');

-- 7. fact_claims
CREATE OR REPLACE TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_claims (
  claim_id STRING NOT NULL COMMENT 'Unique claim identifier',
  member_id STRING NOT NULL COMMENT 'Member identifier linking to dim_member',
  provider_npi STRING COMMENT 'Rendering provider NPI linking to dim_provider',
  service_date DATE NOT NULL COMMENT 'Date of service for the claim',
  claim_type STRING NOT NULL COMMENT 'Claim type classification: IP (Inpatient), OP (Outpatient), Prof (Professional), Rx (Pharmacy)',
  dx_codes ARRAY<STRING> COMMENT 'Array of ICD-10-CM diagnosis codes on the claim',
  proc_codes ARRAY<STRING> COMMENT 'Array of CPT/HCPCS procedure codes on the claim',
  revenue_code STRING COMMENT 'Revenue code for institutional claims',
  paid_amount DECIMAL(12,2) COMMENT 'Total paid amount for the claim in USD',
  measurement_year INT NOT NULL COMMENT 'Measurement year derived from service date'
)
USING DELTA
COMMENT 'Claims detail fact table sourced from 835/837 EDI transactions. Supports denominator and numerator logic derivation from administrative data for quality measure calculations. Contains clinical codes (ICD-10, CPT) and financial data.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality' = 'gold'
);

ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_claims SET TAGS ('domain' = 'claims', 'data_classification' = 'restricted', 'contains_phi' = 'true', 'contains_pii' = 'true', 'hipaa' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_claims ALTER COLUMN member_id SET TAGS ('pii' = 'true', 'phi' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_claims ALTER COLUMN dx_codes SET TAGS ('phi' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_claims ALTER COLUMN service_date SET TAGS ('phi' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_claims ALTER COLUMN paid_amount SET TAGS ('pii' = 'true');
