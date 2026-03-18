-- Tags for dim_county (public reference data)
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_county SET TAGS ('domain' = 'quality', 'phi' = 'false');

-- Tags for dim_provider (provider PII)
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_provider SET TAGS ('domain' = 'operations', 'data_classification' = 'pii', 'phi' = 'false');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_provider ALTER COLUMN provider_npi SET TAGS ('pii' = 'ssn');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_provider ALTER COLUMN provider_name SET TAGS ('pii' = 'ssn');

-- Tags for dim_member (member PHI/PII - HIPAA)
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member SET TAGS ('domain' = 'quality', 'data_classification' = 'pii', 'phi' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN member_id SET TAGS ('phi' = 'member_id');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN date_of_birth SET TAGS ('phi' = 'date_of_birth', 'pii' = 'ssn');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN gender SET TAGS ('phi' = 'demographics');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN race_ethnicity SET TAGS ('phi' = 'demographics');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN zip_code SET TAGS ('pii' = 'address', 'phi' = 'zip_code');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_member ALTER COLUMN chronic_condition_flags SET TAGS ('phi' = 'diagnosis');

-- Tags for dim_measure (measure definitions - internal)
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.dim_measure SET TAGS ('domain' = 'quality', 'phi' = 'false');

-- Tags for fact_quality_events (PHI - clinical events)
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_quality_events SET TAGS ('domain' = 'quality', 'data_classification' = 'pii', 'phi' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_quality_events ALTER COLUMN member_id SET TAGS ('phi' = 'member_id');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_quality_events ALTER COLUMN service_date SET TAGS ('phi' = 'service_date');

-- Tags for fact_enrollment (PHI - enrollment data)
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_enrollment SET TAGS ('domain' = 'quality', 'data_classification' = 'pii', 'phi' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_enrollment ALTER COLUMN member_id SET TAGS ('phi' = 'member_id');

-- Tags for fact_claims (PHI - claims with clinical codes)
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_claims SET TAGS ('domain' = 'quality', 'data_classification' = 'pii', 'phi' = 'true');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_claims ALTER COLUMN member_id SET TAGS ('phi' = 'member_id');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_claims ALTER COLUMN dx_codes SET TAGS ('phi' = 'diagnosis');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_claims ALTER COLUMN service_date SET TAGS ('phi' = 'service_date');
ALTER TABLE serverless_stable_swv01_catalog.medicaid_clinical.fact_claims ALTER COLUMN paid_amount SET TAGS ('phi' = 'financial');
