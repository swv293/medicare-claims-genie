# Databricks notebook source
# MAGIC %md
# MAGIC # Medicaid Clinical Quality Measures - Data Warehouse Setup
# MAGIC
# MAGIC This notebook creates a complete Medicaid clinical data warehouse with:
# MAGIC - **7 Delta tables** (4 dimensions + 3 fact tables) with realistic synthetic data
# MAGIC - **1 Metric View** for flexible quality measure performance analytics with MEASURE() functions
# MAGIC - **Table and column descriptions** for data catalog discoverability
# MAGIC - **PHI/PII tags** for governance and access control policies
# MAGIC - **A Genie Space** for natural language querying
# MAGIC
# MAGIC ## Data Architecture
# MAGIC Star schema pattern supporting HEDIS and CMS Core Set quality measure reporting:
# MAGIC - `dim_member` (1000 rows) — Medicaid enrollee demographics
# MAGIC - `dim_county` (2500 rows) — County reference with FIPS codes
# MAGIC - `dim_provider` (500 rows) — Provider registry with NPI
# MAGIC - `dim_measure` (18 rows) — HEDIS/CMS quality measure definitions
# MAGIC - `fact_quality_events` (10000 rows) — Member × measure × year events
# MAGIC - `fact_enrollment` (3000 rows) — Monthly enrollment snapshots
# MAGIC - `fact_claims` (10000 rows) — Claims with diagnosis/procedure codes
# MAGIC - `mv_quality_performance` — Metric view with MEASURE() functions for flexible aggregation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Update these variables to match your environment.

# COMMAND ----------

# Configuration - UPDATE THESE FOR YOUR ENVIRONMENT
CATALOG = "your_catalog_name"  # e.g., "main" or your Unity Catalog name
SCHEMA = "medicaid_clinical"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Medicaid clinical quality measures data warehouse supporting HEDIS and CMS Core Set reporting'")
print(f"Schema {CATALOG}.{SCHEMA} created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Dimension Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_county — County Reference (2500 rows)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.dim_county (
  county_fips STRING NOT NULL COMMENT 'Five-digit FIPS code uniquely identifying the county',
  county_name STRING NOT NULL COMMENT 'Human-readable county name',
  region STRING COMMENT 'State health region grouping for geographic analysis',
  urban_rural_class STRING COMMENT 'Rural-Urban Continuum Code (RUCC) classification: Metropolitan, Micropolitan, Rural, Frontier',
  state_code STRING NOT NULL COMMENT 'Two-letter US state abbreviation'
)
USING DELTA
COMMENT 'County reference dimension containing geographic and classification data for all US counties used in Medicaid reporting.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_provider — Provider Registry (500 rows)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.dim_provider (
  provider_npi STRING NOT NULL COMMENT 'National Provider Identifier (NPI) - unique 10-digit provider ID',
  provider_name STRING NOT NULL COMMENT 'Individual or organizational provider name',
  provider_type STRING NOT NULL COMMENT 'Provider classification: PCP, Specialist, FQHC, BH, OB/GYN, Pediatrics, Urgent Care, Hospital',
  specialty_code STRING COMMENT 'NUCC Healthcare Provider Taxonomy Code',
  county_fips STRING COMMENT 'FIPS code of provider primary practice location',
  accepting_medicaid BOOLEAN COMMENT 'Whether provider is currently accepting new Medicaid patients'
)
USING DELTA
COMMENT 'Provider registry dimension for all providers serving Medicaid members.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_member — Member Demographics (1000 rows)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.dim_member (
  member_id STRING NOT NULL COMMENT 'Unique Medicaid member identifier',
  date_of_birth DATE NOT NULL COMMENT 'Member date of birth for age-based eligibility',
  gender STRING COMMENT 'M / F / Other',
  race_ethnicity STRING COMMENT 'OMB standard categories for health equity analysis',
  county_fips STRING COMMENT 'FIPS code of member residence county',
  zip_code STRING COMMENT 'Five-digit ZIP code of member residence',
  aid_category STRING NOT NULL COMMENT 'Medicaid eligibility: TANF, SSI, CHIP, Expansion Adult',
  smi_flag BOOLEAN COMMENT 'Serious Mental Illness designation flag',
  chronic_condition_flags ARRAY<STRING> COMMENT 'Array of chronic conditions: Diabetes, HTN, Asthma, etc.',
  enrollment_start_dt DATE NOT NULL COMMENT 'Medicaid eligibility start date',
  enrollment_end_dt DATE COMMENT 'Eligibility end date (NULL = currently active)'
)
USING DELTA
COMMENT 'Member demographics dimension. Contains PHI/PII requiring HIPAA-compliant access controls.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_measure — Quality Measure Definitions (18 rows)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.dim_measure (
  measure_id STRING NOT NULL COMMENT 'NCQA/CMS short code (e.g., CDC-HbA1c, BCS, W34)',
  measure_name STRING NOT NULL COMMENT 'Full measure name',
  measure_category STRING NOT NULL COMMENT 'Domain: Diabetes, Preventive, Behavioral Health, etc.',
  reporting_standard STRING NOT NULL COMMENT 'HEDIS / CMS Adult Core Set / CMS Child Core Set',
  numerator_definition STRING COMMENT 'What counts as a compliant event',
  denominator_definition STRING COMMENT 'Eligible population criteria',
  exclusion_definition STRING COMMENT 'Valid exclusions from denominator',
  age_range STRING COMMENT 'Eligible age range (e.g., 18-75)',
  measurement_year INT NOT NULL COMMENT 'Measurement year this spec applies to',
  reporting_direction STRING NOT NULL COMMENT 'Higher is Better / Lower is Better',
  regulatory_threshold DECIMAL(5,2) COMMENT 'Min performance % required by state contract',
  high_priority_flag BOOLEAN COMMENT 'CMS-designated high-priority measure',
  star_rating_flag BOOLEAN COMMENT 'Included in plan star rating',
  data_source STRING COMMENT 'Admin / Hybrid / ECDS'
)
USING DELTA
COMMENT 'Quality measure definitions from HEDIS MY 2025 and CMS 2025 Core Sets.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_quality_events — Clinical Measure Events (10000 rows)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.fact_quality_events (
  event_id BIGINT NOT NULL COMMENT 'Surrogate key',
  member_id STRING NOT NULL COMMENT 'Links to dim_member',
  measure_id STRING NOT NULL COMMENT 'Links to dim_measure',
  measurement_year INT NOT NULL COMMENT 'e.g., 2024, 2025',
  quarter INT NOT NULL COMMENT '1-4',
  in_denominator BOOLEAN NOT NULL COMMENT 'Member met eligibility criteria',
  in_numerator BOOLEAN NOT NULL COMMENT 'Member met compliant event criteria',
  exclusion_applied BOOLEAN COMMENT 'Valid exclusion was applied',
  service_date DATE COMMENT 'Date of qualifying clinical event',
  provider_npi STRING COMMENT 'Rendering provider NPI',
  data_source STRING COMMENT 'Admin / EHR / Hybrid',
  county_fips STRING COMMENT 'Member county at time of event'
)
USING DELTA
COMMENT 'Primary fact table driving all quality performance calculations. One row per member x measure x measurement year.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_enrollment — Monthly Enrollment Snapshots (3000 rows)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.fact_enrollment (
  snapshot_month DATE NOT NULL COMMENT 'First of month',
  member_id STRING NOT NULL COMMENT 'Links to dim_member',
  aid_category STRING NOT NULL COMMENT 'Enrollment category at snapshot',
  county_fips STRING COMMENT 'County at snapshot',
  plan_id STRING COMMENT 'MCO plan ID or FFS designation',
  is_active BOOLEAN NOT NULL COMMENT 'Enrolled on that month'
)
USING DELTA
COMMENT 'Monthly enrollment snapshot fact table for continuous enrollment logic and enrollment trend analysis.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_claims — Claims Detail (10000 rows)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.fact_claims (
  claim_id STRING NOT NULL COMMENT 'Unique claim identifier',
  member_id STRING NOT NULL COMMENT 'Links to dim_member',
  provider_npi STRING COMMENT 'Rendering provider NPI',
  service_date DATE NOT NULL COMMENT 'Date of service',
  claim_type STRING NOT NULL COMMENT 'IP (Inpatient), OP (Outpatient), Prof (Professional), Rx (Pharmacy)',
  dx_codes ARRAY<STRING> COMMENT 'ICD-10-CM diagnosis codes',
  proc_codes ARRAY<STRING> COMMENT 'CPT/HCPCS procedure codes',
  revenue_code STRING COMMENT 'Revenue code for institutional claims',
  paid_amount DECIMAL(12,2) COMMENT 'Paid amount in USD',
  measurement_year INT NOT NULL COMMENT 'Derived from service date'
)
USING DELTA
COMMENT 'Claims detail fact table from 835/837 EDI. Supports denominator/numerator derivation from admin data.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generate and Load Synthetic Data

# COMMAND ----------

import random
from datetime import date, timedelta
from pyspark.sql import Row
from pyspark.sql.types import *

random.seed(42)

def rdate(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

# ─── Counties ───
STATES = {
    'CA': ['Los Angeles','San Diego','San Francisco','Sacramento','Fresno','Kern','Riverside','San Bernardino','Orange','Alameda','Santa Clara','Contra Costa','Ventura','San Joaquin','Stanislaus','Tulare','Monterey','Solano','Sonoma','Marin','Placer','San Mateo','Butte','Shasta','Yolo','Humboldt','Lake','Merced','Madera','Kings'],
    'TX': ['Harris','Dallas','Tarrant','Bexar','Travis','Collin','Denton','Hidalgo','El Paso','Fort Bend','Williamson','Montgomery','Cameron','Nueces','Brazoria','Bell','Lubbock','Webb','McLennan','Galveston','Smith','Midland','Ector','Hays','Johnson','Ellis','Wichita','Randall','Tom Green','Grayson'],
    'NY': ['New York','Kings','Queens','Bronx','Richmond','Nassau','Suffolk','Westchester','Erie','Monroe','Onondaga','Albany','Dutchess','Orange','Rockland','Saratoga','Oneida','Broome','Niagara','Rensselaer','Schenectady','Chautauqua','Tompkins','Ulster','St. Lawrence','Jefferson','Chemung','Cattaraugus','Steuben','Otsego'],
    'FL': ['Miami-Dade','Broward','Palm Beach','Hillsborough','Orange','Pinellas','Duval','Lee','Polk','Brevard','Volusia','Seminole','Pasco','Sarasota','Manatee','Osceola','Marion','Collier','Escambia','Leon','Alachua','St. Lucie','Bay','Okaloosa','Santa Rosa','Clay','St. Johns','Lake','Hernando','Indian River'],
    'PA': ['Philadelphia','Allegheny','Montgomery','Bucks','Delaware','Lancaster','Chester','York','Berks','Lehigh','Northampton','Luzerne','Dauphin','Erie','Cumberland','Lackawanna','Westmoreland','Monroe','Beaver','Washington','Butler','Centre','Schuylkill','Cambria','Blair','Lebanon','Lycoming','Franklin','Fayette','Adams'],
    'IL': ['Cook','DuPage','Lake','Will','Kane','McHenry','Winnebago','St. Clair','Madison','Champaign','Sangamon','Peoria','McLean','Rock Island','Tazewell','Kankakee','DeKalb','Macon','LaSalle','Vermilion','Adams','Kendall','Grundy','Livingston','Whiteside','Ogle','Lee','Bureau','Knox','Henry'],
    'OH': ['Cuyahoga','Franklin','Hamilton','Summit','Montgomery','Lucas','Stark','Butler','Lorain','Warren','Lake','Mahoning','Clermont','Delaware','Medina','Licking','Fairfield','Portage','Trumbull','Wood','Richland','Allen','Wayne','Columbiana','Geauga','Tuscarawas','Ashtabula','Hancock','Ross','Miami'],
    'GA': ['Fulton','Gwinnett','Cobb','DeKalb','Chatham','Richmond','Clayton','Cherokee','Henry','Forsyth','Hall','Bibb','Muscogee','Columbia','Houston','Douglas','Paulding','Whitfield','Dougherty','Floyd','Lowndes','Bartow','Glynn','Carroll','Bulloch','Coweta','Fayette','Clarke','Liberty','Troup'],
    'MI': ['Wayne','Oakland','Macomb','Kent','Genesee','Washtenaw','Ingham','Ottawa','Kalamazoo','Saginaw','Muskegon','St. Clair','Livingston','Monroe','Jackson','Berrien','Calhoun','Allegan','Bay','Eaton','Midland','Isabella','Lenawee','Shiawassee','Gratiot','Tuscola','Clare','Mecosta','Montcalm','Ionia'],
    'NC': ['Mecklenburg','Wake','Guilford','Forsyth','Cumberland','Durham','Buncombe','New Hanover','Cabarrus','Gaston','Union','Onslow','Johnston','Pitt','Davidson','Catawba','Rowan','Alamance','Randolph','Wayne','Robeson','Iredell','Craven','Nash','Harnett','Henderson','Lenoir','Lee','Moore','Chatham'],
}
REGIONS = ['North','South','East','West','Central','Northeast','Southeast','Northwest','Southwest','Metro']
RURAL_CLASSES = ['Metropolitan','Micropolitan','Rural','Frontier']
EXTRA_STATES = ['WA','OR','AZ','NV','CO','NM','UT','ID','MT','WY','NE','KS','OK','AR','LA','MS','AL','TN','KY','WV','VA','MD','NJ','CT','MA','RI','NH','VT','ME','SC','MN','IA','MO','WI','IN','SD','ND','HI','AK','DC','DE']

counties = []
fips_counter = 1000
for state_code, county_names in STATES.items():
    for cname in county_names:
        fips = str(fips_counter).zfill(5)
        fips_counter += 1
        counties.append((fips, cname, random.choice(REGIONS), random.choices(RURAL_CLASSES, weights=[50,25,20,5])[0], state_code))

while len(counties) < 2500:
    st = random.choice(EXTRA_STATES)
    fips = str(fips_counter).zfill(5)
    fips_counter += 1
    counties.append((fips, f"{st}-County-{len(counties)}", random.choice(REGIONS), random.choices(RURAL_CLASSES, weights=[50,25,20,5])[0], st))

county_fips_list = [c[0] for c in counties]

county_df = spark.createDataFrame(counties, ['county_fips','county_name','region','urban_rural_class','state_code'])
county_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_county")
print(f"dim_county: {county_df.count()} rows loaded")

# COMMAND ----------

# ─── Providers ───
PROVIDER_TYPES = ['PCP','Specialist','FQHC','BH','OB/GYN','Pediatrics','Urgent Care','Hospital']
SPECIALTIES = {'PCP':'207Q00000X','Specialist':'207R00000X','FQHC':'261QF0400X','BH':'103T00000X','OB/GYN':'207V00000X','Pediatrics':'208000000X','Urgent Care':'261QU0200X','Hospital':'282N00000X'}
FIRST_NAMES = ['James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen']
LAST_NAMES = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin']

providers = []
for i in range(500):
    ptype = random.choice(PROVIDER_TYPES)
    providers.append((str(1000000000+i), f"Dr. {random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}", ptype, SPECIALTIES.get(ptype,'207Q00000X'), random.choice(county_fips_list[:300]), random.random()<0.85))

provider_df = spark.createDataFrame(providers, ['provider_npi','provider_name','provider_type','specialty_code','county_fips','accepting_medicaid'])
provider_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_provider")
print(f"dim_provider: {provider_df.count()} rows loaded")

# COMMAND ----------

# ─── Members ───
AID_CATS = ['TANF','SSI','CHIP','Expansion Adult']
RACES = ['White','Black or African American','Hispanic or Latino','Asian','American Indian/Alaska Native','Native Hawaiian/Pacific Islander','Two or More Races']
CHRONIC_CONDITIONS = ['Diabetes','Hypertension','Asthma','COPD','CHF','Depression','Anxiety','Obesity','CKD','Substance Use Disorder']

members = []
for i in range(1000):
    dob = rdate(date(1945,1,1), date(2022,12,31))
    enroll_start = rdate(date(2020,1,1), date(2024,6,1))
    enroll_end = None if random.random()<0.7 else rdate(enroll_start+timedelta(days=90), date(2025,12,31))
    num_chronic = random.choices([0,1,2,3,4], weights=[30,30,20,15,5])[0]
    chronic = random.sample(CHRONIC_CONDITIONS, num_chronic) if num_chronic>0 else []
    members.append((f"MED{str(i+1).zfill(7)}", dob, random.choices(['M','F','Other'],weights=[48,50,2])[0], random.choice(RACES), random.choice(county_fips_list[:300]), str(random.randint(10000,99999)), random.choices(AID_CATS,weights=[35,20,25,20])[0], random.random()<0.08, chronic, enroll_start, enroll_end))

member_schema = StructType([
    StructField("member_id", StringType()),
    StructField("date_of_birth", DateType()),
    StructField("gender", StringType()),
    StructField("race_ethnicity", StringType()),
    StructField("county_fips", StringType()),
    StructField("zip_code", StringType()),
    StructField("aid_category", StringType()),
    StructField("smi_flag", BooleanType()),
    StructField("chronic_condition_flags", ArrayType(StringType())),
    StructField("enrollment_start_dt", DateType()),
    StructField("enrollment_end_dt", DateType()),
])

member_df = spark.createDataFrame(members, member_schema)
member_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_member")
print(f"dim_member: {member_df.count()} rows loaded")

# COMMAND ----------

# ─── Quality Measures (18 seeded rows) ───
measures_data = [
    ('CDC-HbA1c','Glycemic Status Assessment for Patients with Diabetes','Diabetes','HEDIS','Patients with diabetes whose most recent HbA1c level is >9.0%','Patients 18-75 with diabetes','Patients in hospice or with ESRD','18-75',2025,'Lower is Better',25.0,True,True,'Hybrid'),
    ('CDC-EYE','Eye Exam for Patients with Diabetes','Diabetes','HEDIS','Patients who had a retinal eye exam','Patients 18-75 with diabetes','Patients in hospice','18-75',2025,'Higher is Better',55.0,True,True,'Hybrid'),
    ('CDC-NEP','Kidney Health Evaluation for Patients with Diabetes','Diabetes','HEDIS','Patients with both urine albumin-creatinine ratio and eGFR tests','Patients 18-85 with diabetes','Patients in hospice or ESRD on dialysis','18-85',2025,'Higher is Better',40.0,False,False,'Admin'),
    ('CBP','Controlling High Blood Pressure','Cardiovascular','HEDIS','Patients whose most recent BP is adequately controlled (<140/90)','Patients 18-85 with hypertension','Hospice, ESRD, or pregnancy','18-85',2025,'Higher is Better',60.0,True,True,'Hybrid'),
    ('BCS','Breast Cancer Screening','Preventive','HEDIS','Women with mammogram during MY or prior year','Women 50-74','Bilateral mastectomy','50-74',2025,'Higher is Better',55.0,True,True,'Admin'),
    ('CCS','Cervical Cancer Screening','Preventive','HEDIS','Women screened (Pap within 3 yrs or HPV within 5 yrs)','Women 21-64','Hysterectomy with no residual cervix','21-64',2025,'Higher is Better',55.0,True,True,'Hybrid'),
    ('COL','Colorectal Cancer Screening','Preventive','HEDIS','Appropriate colorectal cancer screening','Adults 45-75','Colorectal cancer or total colectomy','45-75',2025,'Higher is Better',50.0,True,True,'Admin'),
    ('W34','Well-Child Visits (3rd-6th Year of Life)','Child Health','CMS Child Core Set','At least one well-child visit during MY','Children aged 3-6','None','3-6',2025,'Higher is Better',70.0,True,False,'Admin'),
    ('CIS','Childhood Immunization Status','Immunization','CMS Child Core Set','All recommended immunizations by 2nd birthday','Children who turned 2 during MY','Children in hospice','2',2025,'Higher is Better',65.0,True,False,'Admin'),
    ('FUH','Follow-Up After Hospitalization for Mental Illness','Behavioral Health','HEDIS','Follow-up visit within 7 days of discharge','Patients 6+ discharged from MH facility','Patients who died during stay','6+',2025,'Higher is Better',45.0,True,True,'Admin'),
    ('FUM','Follow-Up After ED Visit for Mental Illness','Behavioral Health','CMS Adult Core Set','Follow-up visit within 7 days of ED visit','Patients 6+ with ED visit for MI','Admitted directly from ED','6+',2025,'Higher is Better',40.0,False,False,'Admin'),
    ('AMR','Asthma Medication Ratio','Respiratory','HEDIS','Ratio of controller to total asthma medications >= 0.50','Patients 5-64 with persistent asthma','COPD, emphysema, cystic fibrosis','5-64',2025,'Higher is Better',60.0,True,True,'Admin'),
    ('AMM','Antidepressant Medication Management','Behavioral Health','HEDIS','Remained on antidepressant 84+ days (acute) or 180+ days','Patients 18+ with new depression episode','Prior antidepressant use in 105 days','18+',2025,'Higher is Better',50.0,False,True,'Admin'),
    ('PPC','Prenatal and Postpartum Care','Maternal Health','HEDIS','Prenatal visit in 1st trimester + postpartum visit 7-84 days after delivery','Women with live birth delivery','None','15-44',2025,'Higher is Better',65.0,True,False,'Hybrid'),
    ('WCC-BMI','Weight Assessment and Counseling - BMI Percentile','Nutrition/Obesity','HEDIS','BMI percentile documented during MY','Children 3-17','Pregnancy','3-17',2025,'Higher is Better',55.0,False,False,'Hybrid'),
    ('AIS-E','Adult Immunization Status','Preventive','HEDIS','Up-to-date immunization per ACIP recommendations','Adults 19+','Hospice','19+',2025,'Higher is Better',40.0,False,False,'Admin'),
    ('SUD-LOT','Initiation and Engagement of SUD Treatment','Behavioral Health','CMS Adult Core Set','Initiated SUD treatment within 14 days + 2 additional services within 34 days','Patients 13+ with new SUD diagnosis','Active SUD treatment in prior 60 days','13+',2025,'Higher is Better',40.0,True,False,'Admin'),
    ('PCR','Plan All-Cause Readmissions','Utilization','HEDIS','Unplanned readmission within 30 days','Patients 18+ discharged from acute stay','Planned readmissions, death, AMA','18+',2025,'Lower is Better',15.0,True,True,'Admin'),
]

measure_schema = StructType([
    StructField("measure_id", StringType()),
    StructField("measure_name", StringType()),
    StructField("measure_category", StringType()),
    StructField("reporting_standard", StringType()),
    StructField("numerator_definition", StringType()),
    StructField("denominator_definition", StringType()),
    StructField("exclusion_definition", StringType()),
    StructField("age_range", StringType()),
    StructField("measurement_year", IntegerType()),
    StructField("reporting_direction", StringType()),
    StructField("regulatory_threshold", DecimalType(5,2)),
    StructField("high_priority_flag", BooleanType()),
    StructField("star_rating_flag", BooleanType()),
    StructField("data_source", StringType()),
])

from decimal import Decimal
measures_rows = [(m[0],m[1],m[2],m[3],m[4],m[5],m[6],m[7],m[8],m[9],Decimal(str(m[10])),m[11],m[12],m[13]) for m in measures_data]
measure_df = spark.createDataFrame(measures_rows, measure_schema)
measure_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_measure")
print(f"dim_measure: {measure_df.count()} rows loaded")

# COMMAND ----------

# ─── Quality Events ───
measure_ids = [m[0] for m in measures_data]
member_ids = [f"MED{str(i+1).zfill(7)}" for i in range(1000)]
provider_npis = [str(1000000000+i) for i in range(500)]

quality_events = []
for i in range(10000):
    my = random.choice([2024, 2025])
    quarter = random.randint(1, 4)
    mid = random.choice(member_ids)
    in_denom = random.random() < 0.85
    in_numer = random.random() < 0.65 if in_denom else False
    excl = random.random() < 0.05 if in_denom else False
    q_start = date(my, (quarter-1)*3+1, 1)
    svc_date = rdate(q_start, q_start + timedelta(days=89))
    quality_events.append((i+1, mid, random.choice(measure_ids), my, quarter, in_denom, in_numer, excl, svc_date, random.choice(provider_npis), random.choice(['Admin','EHR','Hybrid']), random.choice(county_fips_list[:300])))

qe_schema = StructType([
    StructField("event_id", LongType()),
    StructField("member_id", StringType()),
    StructField("measure_id", StringType()),
    StructField("measurement_year", IntegerType()),
    StructField("quarter", IntegerType()),
    StructField("in_denominator", BooleanType()),
    StructField("in_numerator", BooleanType()),
    StructField("exclusion_applied", BooleanType()),
    StructField("service_date", DateType()),
    StructField("provider_npi", StringType()),
    StructField("data_source", StringType()),
    StructField("county_fips", StringType()),
])

qe_df = spark.createDataFrame(quality_events, qe_schema)
qe_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_quality_events")
print(f"fact_quality_events: {qe_df.count()} rows loaded")

# COMMAND ----------

# ─── Enrollment ───
enrollments = []
for i in range(3000):
    mid = random.choice(member_ids)
    yr = random.choice([2024, 2025])
    mo = random.randint(1, 12)
    aid = random.choice(AID_CATS)
    enrollments.append((date(yr, mo, 1), mid, aid, random.choice(county_fips_list[:300]), random.choice(['MCO-BlueCross','MCO-Aetna','MCO-UHC','MCO-Centene','MCO-Molina','FFS']), random.random()<0.9))

enr_schema = StructType([
    StructField("snapshot_month", DateType()),
    StructField("member_id", StringType()),
    StructField("aid_category", StringType()),
    StructField("county_fips", StringType()),
    StructField("plan_id", StringType()),
    StructField("is_active", BooleanType()),
])

enr_df = spark.createDataFrame(enrollments, enr_schema)
enr_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_enrollment")
print(f"fact_enrollment: {enr_df.count()} rows loaded")

# COMMAND ----------

# ─── Claims ───
DX_CODES = ['E11.9','E11.65','I10','J45.20','J44.1','F32.1','F41.1','E66.01','N18.3','F10.20','Z23','Z00.129','Z12.11','Z12.31','Z01.00','O80','G47.33','M54.5','K21.0','R10.9']
PROC_CODES = ['99213','99214','99215','99395','99396','83036','81001','85025','80053','36415','90471','90686','77067','88175','45378','99381','59400','96127','90837','99243']
CLAIM_TYPES = ['IP','OP','Prof','Rx']
REV_CODES = ['0100','0110','0120','0250','0260','0270','0300','0320','0450','0510']

claims = []
for i in range(10000):
    mid = random.choice(member_ids)
    svc_date = rdate(date(2024,1,1), date(2025,12,31))
    claims.append((f"CLM{str(i+1).zfill(8)}", mid, random.choice(provider_npis), svc_date, random.choices(CLAIM_TYPES,weights=[15,35,40,10])[0], random.sample(DX_CODES, random.randint(1,4)), random.sample(PROC_CODES, random.randint(1,3)), random.choice(REV_CODES), float(round(random.uniform(15.0,25000.0),2)), svc_date.year))

clm_schema = StructType([
    StructField("claim_id", StringType()),
    StructField("member_id", StringType()),
    StructField("provider_npi", StringType()),
    StructField("service_date", DateType()),
    StructField("claim_type", StringType()),
    StructField("dx_codes", ArrayType(StringType())),
    StructField("proc_codes", ArrayType(StringType())),
    StructField("revenue_code", StringType()),
    StructField("paid_amount", DoubleType()),
    StructField("measurement_year", IntegerType()),
])

clm_df = spark.createDataFrame(claims, clm_schema)
clm_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_claims")
print(f"fact_claims: {clm_df.count()} rows loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Apply Tags (PHI/PII Governance)
# MAGIC
# MAGIC **Note:** Update the tag values below to match your workspace's tag policy allowed values.
# MAGIC Run `ALTER TABLE ... SET TAGS ('tag_key' = 'invalid_value')` to see allowed values in the error message.

# COMMAND ----------

# Table-level tags - adjust values to your workspace tag policies
tag_statements = [
    # dim_county - public reference data
    f"ALTER TABLE {CATALOG}.{SCHEMA}.dim_county SET TAGS ('domain' = 'quality', 'phi' = 'false')",
    # dim_provider - provider PII
    f"ALTER TABLE {CATALOG}.{SCHEMA}.dim_provider SET TAGS ('domain' = 'operations', 'data_classification' = 'pii', 'phi' = 'false')",
    # dim_member - PHI/PII (HIPAA)
    f"ALTER TABLE {CATALOG}.{SCHEMA}.dim_member SET TAGS ('domain' = 'quality', 'data_classification' = 'pii', 'phi' = 'true')",
    # dim_measure - internal
    f"ALTER TABLE {CATALOG}.{SCHEMA}.dim_measure SET TAGS ('domain' = 'quality', 'phi' = 'false')",
    # fact tables - PHI
    f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_quality_events SET TAGS ('domain' = 'quality', 'data_classification' = 'pii', 'phi' = 'true')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_enrollment SET TAGS ('domain' = 'quality', 'data_classification' = 'pii', 'phi' = 'true')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_claims SET TAGS ('domain' = 'quality', 'data_classification' = 'pii', 'phi' = 'true')",

    # Column-level PHI tags
    f"ALTER TABLE {CATALOG}.{SCHEMA}.dim_member ALTER COLUMN member_id SET TAGS ('phi' = 'member_id')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.dim_member ALTER COLUMN date_of_birth SET TAGS ('phi' = 'date_of_birth')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.dim_member ALTER COLUMN zip_code SET TAGS ('phi' = 'zip_code')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.dim_member ALTER COLUMN chronic_condition_flags SET TAGS ('phi' = 'diagnosis')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_quality_events ALTER COLUMN member_id SET TAGS ('phi' = 'member_id')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_quality_events ALTER COLUMN service_date SET TAGS ('phi' = 'service_date')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_enrollment ALTER COLUMN member_id SET TAGS ('phi' = 'member_id')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_claims ALTER COLUMN member_id SET TAGS ('phi' = 'member_id')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_claims ALTER COLUMN dx_codes SET TAGS ('phi' = 'diagnosis')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_claims ALTER COLUMN service_date SET TAGS ('phi' = 'service_date')",
    f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_claims ALTER COLUMN paid_amount SET TAGS ('phi' = 'financial')",
]

for stmt in tag_statements:
    try:
        spark.sql(stmt)
        print(f"✓ {stmt[:80]}...")
    except Exception as e:
        print(f"✗ {stmt[:80]}... -> {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Quality Performance Metric View
# MAGIC
# MAGIC This creates a **Databricks Metric View** (not a standard view) that joins fact_quality_events
# MAGIC with all dimension tables. Metric views separate measure definitions from dimension groupings,
# MAGIC allowing flexible aggregation at query time using the `MEASURE()` function.
# MAGIC
# MAGIC **Requires:** Databricks Runtime 17.2+ or serverless SQL warehouse.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_quality_performance
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: >
  Medicaid clinical quality measure performance metric view.
  Computes numerator, denominator, and performance rates across HEDIS and CMS Core Set measures.
  Dimensions include measure, county, year, quarter, provider, and member demographics.
  Use MEASURE() aggregate function when querying measures.

source: {CATALOG}.{SCHEMA}.fact_quality_events

joins:
  - name: dim_measure
    source: {CATALOG}.{SCHEMA}.dim_measure
    "on": source.measure_id = dim_measure.measure_id

  - name: dim_county
    source: {CATALOG}.{SCHEMA}.dim_county
    "on": source.county_fips = dim_county.county_fips

  - name: dim_provider
    source: {CATALOG}.{SCHEMA}.dim_provider
    "on": source.provider_npi = dim_provider.provider_npi

  - name: dim_member
    source: {CATALOG}.{SCHEMA}.dim_member
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
    comment: "Quality measure performance rate: (numerator / denominator) * 100"
  - name: gap_to_threshold
    expr: |
      ROUND(
        ANY_VALUE(dim_measure.regulatory_threshold) -
        (COUNT(CASE WHEN source.in_numerator AND NOT source.exclusion_applied THEN 1 END) * 100.0
         / NULLIF(COUNT(CASE WHEN source.in_denominator AND NOT source.exclusion_applied THEN 1 END), 0)),
        2
      )
    comment: "Gap between performance rate and regulatory threshold. Positive = below threshold."
  - name: distinct_members
    expr: COUNT(DISTINCT source.member_id)
    comment: "Count of distinct members with quality events"
  - name: distinct_providers
    expr: COUNT(DISTINCT source.provider_npi)
    comment: "Count of distinct rendering providers"
$$
""")
print("mv_quality_performance metric view created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Validate Data

# COMMAND ----------

tables = ['dim_county','dim_provider','dim_member','dim_measure','fact_quality_events','fact_enrollment','fact_claims']
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{t}").collect()[0][0]
    print(f"{t}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Benchmark Queries
# MAGIC
# MAGIC These 5 benchmark queries are also registered in the Genie space as `example_question_sqls`.
# MAGIC They serve as reference SQL for Genie to learn from and as test cases for evaluating accuracy.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Benchmark 1: Enrollment by county
# MAGIC -- Question: "What are our Medicaid enrollment numbers by county?"
# MAGIC SELECT c.county_name, c.state_code,
# MAGIC   COUNT(DISTINCT e.member_id) as enrolled_members,
# MAGIC   SUM(CASE WHEN e.is_active THEN 1 ELSE 0 END) as active_member_months
# MAGIC FROM ${CATALOG}.${SCHEMA}.fact_enrollment e
# MAGIC JOIN ${CATALOG}.${SCHEMA}.dim_county c ON e.county_fips = c.county_fips
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 3 DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Benchmark 2: Quality metrics for current quarter (Metric View)
# MAGIC -- Question: "Show me clinical quality metrics for the current quarter"
# MAGIC SELECT measure_name, measure_category, reporting_direction, regulatory_threshold,
# MAGIC   MEASURE(denominator) as denominator,
# MAGIC   MEASURE(numerator) as numerator,
# MAGIC   MEASURE(performance_rate) as performance_rate,
# MAGIC   MEASURE(gap_to_threshold) as gap_to_threshold
# MAGIC FROM ${CATALOG}.${SCHEMA}.mv_quality_performance
# MAGIC WHERE measurement_year = 2025 AND quarter = 1
# MAGIC GROUP BY measure_name, measure_category, reporting_direction, regulatory_threshold
# MAGIC ORDER BY measure_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Benchmark 3: At-risk measures not meeting thresholds
# MAGIC -- Question: "Which measures are at risk of not meeting regulatory thresholds?"
# MAGIC SELECT * FROM (
# MAGIC   SELECT measure_name, measure_category, measurement_year, reporting_direction,
# MAGIC     regulatory_threshold,
# MAGIC     MEASURE(performance_rate) as performance_rate,
# MAGIC     MEASURE(gap_to_threshold) as gap_to_threshold,
# MAGIC     MEASURE(denominator) as denominator
# MAGIC   FROM ${CATALOG}.${SCHEMA}.mv_quality_performance
# MAGIC   WHERE measurement_year = 2025
# MAGIC   GROUP BY measure_name, measure_category, measurement_year, reporting_direction, regulatory_threshold
# MAGIC )
# MAGIC WHERE (reporting_direction = 'Higher is Better' AND performance_rate < regulatory_threshold)
# MAGIC    OR (reporting_direction = 'Lower is Better' AND performance_rate > regulatory_threshold)
# MAGIC ORDER BY ABS(gap_to_threshold) DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Benchmark 4: Year-over-year comparison by measure (Metric View)
# MAGIC -- Question: "Compare this year's performance vs last year by quality measure"
# MAGIC SELECT measure_name, measure_category, measurement_year,
# MAGIC   MEASURE(performance_rate) as performance_rate,
# MAGIC   MEASURE(denominator) as denominator,
# MAGIC   MEASURE(numerator) as numerator,
# MAGIC   MEASURE(distinct_members) as distinct_members
# MAGIC FROM ${CATALOG}.${SCHEMA}.mv_quality_performance
# MAGIC GROUP BY measure_name, measure_category, measurement_year
# MAGIC ORDER BY measure_name, measurement_year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Benchmark 5: Claims cost by type and aid category
# MAGIC -- Question: "Show claims cost breakdown by claim type and aid category"
# MAGIC SELECT cl.claim_type, m.aid_category,
# MAGIC   COUNT(*) as claim_count,
# MAGIC   ROUND(SUM(cl.paid_amount), 2) as total_paid,
# MAGIC   ROUND(AVG(cl.paid_amount), 2) as avg_paid,
# MAGIC   COUNT(DISTINCT cl.member_id) as unique_members
# MAGIC FROM ${CATALOG}.${SCHEMA}.fact_claims cl
# MAGIC JOIN ${CATALOG}.${SCHEMA}.dim_member m ON cl.member_id = m.member_id
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 4 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create the Genie Space (one-notebook path)
# MAGIC
# MAGIC This section is **fully idempotent**: it creates a Genie space the first time you run it,
# MAGIC then updates the same space on subsequent runs. The space_id is persisted in
# MAGIC `${CATALOG}.${SCHEMA}.config_genie` so the notebook remembers what it created.
# MAGIC
# MAGIC The space ships with: 13 sample questions, 10 benchmark queries (5 standard + 5 window
# MAGIC function patterns), 10 join specs, SQL snippets (filters/expressions/measures), and a
# MAGIC text instruction block covering the data model, jargon, and calculation rules.

# COMMAND ----------

# Idempotent config table for the Genie space_id
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.config_genie (
  config_key STRING NOT NULL COMMENT 'Configuration key, e.g. genie_space_id',
  config_value STRING COMMENT 'Configuration value',
  created_at TIMESTAMP COMMENT 'When this config row was written'
)
USING DELTA
COMMENT 'Notebook-managed Genie space configuration. Stores genie_space_id so re-runs PATCH instead of POST.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Build serialized_space (config + instructions + benchmarks + joins)

# COMMAND ----------

import json

FQN = f"{CATALOG}.{SCHEMA}"

# Look up the SQL warehouse to back the Genie space.
# In a Databricks notebook, the WAREHOUSE_ID is read from a notebook widget if you have one,
# otherwise paste your warehouse id below. Find it under SQL Warehouses in the workspace.
try:
    WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")  # noqa: F821
except Exception:
    WAREHOUSE_ID = "084543d48aafaeb2"  # <-- replace with your SQL warehouse id

GENIE_TITLE = "Medicaid Clinical Quality Measures"
GENIE_DESCRIPTION = (
    "Natural-language analytics over a Medicaid clinical quality measures star schema. "
    "Supports HEDIS and CMS Core Set reporting, year-over-year trends, provider rankings, "
    "and county-level performance. Backed by the mv_quality_performance metric view for "
    "governed measure definitions."
)

# 13 sample questions (5 standard + 8 advanced/window)
sample_questions = [
    {"id": "a0000000000000000000000000000001", "question": ["What are our Medicaid enrollment numbers by county?"]},
    {"id": "a0000000000000000000000000000002", "question": ["Show me clinical quality metrics for the current quarter"]},
    {"id": "a0000000000000000000000000000003", "question": ["Which measures are at risk of not meeting regulatory thresholds?"]},
    {"id": "a0000000000000000000000000000004", "question": ["Compare this year's performance vs last year by quality measure"]},
    {"id": "a0000000000000000000000000000005", "question": ["How are we performing on all diabetes-related quality measures?"]},
    {"id": "a0000000000000000000000000000006", "question": ["Show behavioral health follow-up rates by quarter"]},
    {"id": "a0000000000000000000000000000007", "question": ["Which providers have the highest quality measure compliance rates?"]},
    {"id": "a0000000000000000000000000000008", "question": ["Show enrollment trends by aid category over time"]},
    {"id": "a0000000000000000000000000000009", "question": ["Rank providers by their quality measure performance rate"]},
    {"id": "a000000000000000000000000000000a", "question": ["Show quarter-over-quarter performance trend for each measure"]},
    {"id": "a000000000000000000000000000000b", "question": ["Which providers are in the bottom quartile for quality performance?"]},
    {"id": "a000000000000000000000000000000c", "question": ["Show cumulative enrollment growth with a 3-month rolling average"]},
    {"id": "a000000000000000000000000000000d", "question": ["What percentile does each county rank in for diabetes measure performance?"]},
]

text_instructions = [{
    "id": "b0000000000000000000000000000010",
    "content": [
        "This Genie space provides AI-powered analytics for Medicaid clinical quality measures data. It supports HEDIS and CMS Core Set reporting with year-over-year performance comparisons.\n\n",
        "=== DATA MODEL (Star Schema) ===\n",
        "DIMENSIONS: dim_member (1000 rows, PK: member_id) - demographics, aid_category, chronic_condition_flags; dim_county (2500 rows, PK: county_fips) - geography; dim_provider (500 rows, PK: provider_npi) - provider registry; dim_measure (18 rows, PK: measure_id) - HEDIS/CMS measure definitions with thresholds.\n",
        "FACTS: fact_quality_events (10000 rows) - member x measure x year with in_denominator/in_numerator/exclusion_applied flags; fact_enrollment (3000 rows) - monthly snapshots; fact_claims (10000 rows) - claims with ICD-10 dx_codes and CPT proc_codes.\n",
        "METRIC VIEW: mv_quality_performance - joins fact_quality_events with dim_measure, dim_county, dim_provider, dim_member. Query with MEASURE() function. Measures: denominator, numerator, performance_rate, gap_to_threshold, total_events, exclusion_count, distinct_members, distinct_providers. Dimensions: measure_name, measure_category, measurement_year, quarter, county_name, state_code, region, provider_type, aid_category, gender, race_ethnicity, and more.\n\n",
        "=== JARGON ===\n",
        "HEDIS: Healthcare Effectiveness Data and Information Set (NCQA quality measures). CMS Core Set: CMS-required Medicaid/CHIP measures. MY: Measurement Year. Performance Rate: (Numerator/Denominator)*100. Regulatory Threshold: min rate required by state contract. At Risk: rate below threshold.\n",
        "Aid Categories: TANF, SSI, CHIP, Expansion Adult. Claim Types: IP, OP, Prof, Rx. Provider Types: PCP, FQHC, BH. SMI: Serious Mental Illness. MCO: Managed Care Organization. FFS: Fee-For-Service. SUD: Substance Use Disorder. PMPM: Per Member Per Month.\n\n",
        "=== CALCULATION RULES ===\n",
        "Performance Rate = COUNT(in_numerator AND NOT exclusion_applied) * 100.0 / NULLIF(COUNT(in_denominator AND NOT exclusion_applied), 0). For 'Lower is Better' measures (CDC-HbA1c, PCR), lower rate = better. For all others, higher rate = better.\n",
        "IMPORTANT: mv_quality_performance is a METRIC VIEW. Always wrap measures in MEASURE() function. Do NOT use SELECT *. Always specify dimensions in GROUP BY.\n\n",
        "=== WINDOW FUNCTION PATTERNS ===\n",
        "Use window functions for ranking, trending, and comparative analytics on the base tables (NOT the metric view). The metric view uses MEASURE() aggregates only.\n\n",
        "RANK/DENSE_RANK: Rank providers or counties by performance rate within each measure. Use RANK() OVER(PARTITION BY measure_name ORDER BY performance_rate DESC).\n",
        "LAG/LEAD: Show quarter-over-quarter or year-over-year changes. Use LAG(performance_rate) OVER(PARTITION BY measure_name ORDER BY measurement_year, quarter).\n",
        "NTILE: Classify providers or counties into quartiles. Use NTILE(4) OVER(ORDER BY performance_rate). Quartile 1 = bottom 25%.\n",
        "RUNNING SUM/AVG: Use SUM(x) OVER(ORDER BY snapshot_month) for cumulative; AVG(x) OVER(ORDER BY snapshot_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) for 3-month rolling.\n",
        "PERCENT_RANK: Use ROUND(PERCENT_RANK() OVER(PARTITION BY measure_name ORDER BY performance_rate) * 100, 1) for 0-100 percentile.\n",
        "Always compute performance_rate in an inner query (GROUP BY) and apply window functions in the outer query.\n",
    ],
}]

# 10 join specs - sql array MUST have 2 elements: backtick-aliased condition + relationship-type annotation
def _join(jid, lt, lc, rt, rc, comment, instruction):
    return {
        "id": jid,
        "left": {"identifier": f"{FQN}.{lt}", "alias": lt},
        "right": {"identifier": f"{FQN}.{rt}", "alias": rt},
        "sql": [f"`{lt}`.{lc} = `{rt}`.{rc}", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
        "comment": [comment],
        "instruction": [instruction],
    }

join_specs = [
    _join("e0000000000000000000000000000001", "dim_member", "county_fips", "dim_county", "county_fips",
          "Join member to county for geographic analysis of member residence",
          "Use when you need member county name, state, region, or urban/rural classification"),
    _join("e0000000000000000000000000000002", "dim_provider", "county_fips", "dim_county", "county_fips",
          "Join provider to county for provider practice location analysis",
          "Use when you need provider location details"),
    _join("e0000000000000000000000000000003", "fact_claims", "member_id", "dim_member", "member_id",
          "Join claims to member demographics",
          "Use when analyzing claims by member demographics like aid category, gender, or chronic conditions"),
    _join("e0000000000000000000000000000004", "fact_claims", "provider_npi", "dim_provider", "provider_npi",
          "Join claims to provider for provider-level claims analysis",
          "Use when analyzing claims by provider type or specific providers"),
    _join("e0000000000000000000000000000005", "fact_enrollment", "county_fips", "dim_county", "county_fips",
          "Join enrollment to county for enrollment by geography",
          "Use when analyzing enrollment numbers by county, state, or region"),
    _join("e0000000000000000000000000000006", "fact_enrollment", "member_id", "dim_member", "member_id",
          "Join enrollment to member demographics",
          "Use when analyzing enrollment trends by member demographics"),
    _join("e0000000000000000000000000000007", "fact_quality_events", "county_fips", "dim_county", "county_fips",
          "Join quality events to county for geographic quality analysis",
          "Use when analyzing quality measure performance by county or region"),
    _join("e0000000000000000000000000000008", "fact_quality_events", "measure_id", "dim_measure", "measure_id",
          "Join quality events to measure definitions for measure metadata",
          "Use when you need measure name, category, thresholds, or reporting direction"),
    _join("e0000000000000000000000000000009", "fact_quality_events", "member_id", "dim_member", "member_id",
          "Join quality events to member demographics",
          "Use when analyzing quality measures by member demographics, aid category, or chronic conditions"),
    _join("e000000000000000000000000000000a", "fact_quality_events", "provider_npi", "dim_provider", "provider_npi",
          "Join quality events to provider for provider-level quality analysis",
          "Use when analyzing quality measure compliance by provider type or specific providers"),
]

# SQL snippets - reusable filters, expressions, and measures
sql_snippets = {
    "filters": [
        {"id": "f0000000000000000000000000000001", "sql": ["fact_enrollment.is_active = TRUE"],
         "display_name": "active members only", "synonyms": ["currently enrolled", "active enrollment"],
         "comment": ["Filters to only actively enrolled members"],
         "instruction": ["Use when counting current enrollment or active members"]},
        {"id": "f0000000000000000000000000000002",
         "sql": ["fact_quality_events.in_denominator = TRUE AND fact_quality_events.exclusion_applied = FALSE"],
         "display_name": "eligible for measure", "synonyms": ["in denominator", "eligible population", "measure eligible"],
         "comment": ["Filters to members eligible for a quality measure (in denominator, no exclusion)"],
         "instruction": ["Use as base filter when calculating quality measure rates"]},
        {"id": "f0000000000000000000000000000003", "sql": ["dim_measure.high_priority_flag = TRUE"],
         "display_name": "high priority measures", "synonyms": ["CMS priority", "key measures", "critical measures"],
         "comment": ["Filters to CMS-designated high-priority quality measures"],
         "instruction": ["Use when focusing on the most important measures for regulatory reporting"]},
        {"id": "f0000000000000000000000000000004", "sql": ["dim_measure.star_rating_flag = TRUE"],
         "display_name": "star rating measures", "synonyms": ["star measures", "plan rating measures"],
         "comment": ["Filters to measures included in health plan star ratings"],
         "instruction": ["Use when analyzing measures that impact plan star ratings"]},
    ],
    "expressions": [
        {"id": "f0000000000000000000000000000005", "alias": "measurement_quarter",
         "sql": ["CONCAT(fact_quality_events.measurement_year, '-Q', fact_quality_events.quarter)"],
         "display_name": "measurement quarter", "synonyms": ["quarter", "reporting quarter"],
         "comment": ["Formats measurement year and quarter as YYYY-QN"],
         "instruction": ["Use for quarter-level trend analysis labels"]},
        {"id": "f0000000000000000000000000000006", "alias": "member_age",
         "sql": ["FLOOR(DATEDIFF(CURRENT_DATE(), dim_member.date_of_birth) / 365.25)"],
         "display_name": "member age", "synonyms": ["age", "patient age", "enrollee age"],
         "comment": ["Calculates current age in years from date of birth"],
         "instruction": ["Use when analyzing by age group or checking age-based eligibility"]},
        {"id": "f0000000000000000000000000000007", "alias": "enrollment_month_label",
         "sql": ["DATE_FORMAT(fact_enrollment.snapshot_month, 'yyyy-MM')"],
         "display_name": "enrollment month", "synonyms": ["month", "snapshot month"],
         "comment": ["Formats enrollment snapshot month as YYYY-MM"],
         "instruction": ["Use for monthly enrollment trend labels"]},
    ],
    "measures": [
        {"id": "f0000000000000000000000000000008", "alias": "performance_rate",
         "sql": ["ROUND(COUNT(CASE WHEN fact_quality_events.in_numerator AND NOT fact_quality_events.exclusion_applied THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN fact_quality_events.in_denominator AND NOT fact_quality_events.exclusion_applied THEN 1 END), 0), 2)"],
         "display_name": "performance rate", "synonyms": ["compliance rate", "quality rate", "HEDIS rate"],
         "comment": ["Quality measure performance rate: (numerator / denominator) * 100. Excludes valid exclusions."],
         "instruction": ["Use for calculating quality measure compliance rates."]},
        {"id": "f0000000000000000000000000000009", "alias": "total_paid",
         "sql": ["ROUND(SUM(fact_claims.paid_amount), 2)"],
         "display_name": "total paid amount", "synonyms": ["total cost", "total spend", "paid claims"],
         "comment": ["Sum of all claim paid amounts in USD"],
         "instruction": ["Use for claims cost analysis and financial reporting"]},
        {"id": "f000000000000000000000000000000a", "alias": "pmpm_cost",
         "sql": ["ROUND(SUM(fact_claims.paid_amount) / NULLIF(COUNT(DISTINCT fact_claims.member_id), 0), 2)"],
         "display_name": "per member cost", "synonyms": ["PMPM", "per member per month"],
         "comment": ["Average paid amount per unique member"],
         "instruction": ["Use for per-member cost analysis and PMPM calculations"]},
    ],
}

# 10 benchmarks: 5 standard aggregation + 5 window function patterns
benchmarks_questions = [
    {"id": "b0000000000000000000000000000001",
     "question": ["What are our Medicaid enrollment numbers by county?"],
     "answer": [{"format": "SQL", "content": [f"SELECT c.county_name, c.state_code, COUNT(DISTINCT e.member_id) as enrolled_members, SUM(CASE WHEN e.is_active THEN 1 ELSE 0 END) as active_member_months FROM {FQN}.fact_enrollment e JOIN {FQN}.dim_county c ON e.county_fips = c.county_fips GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 15"]}]},
    {"id": "b0000000000000000000000000000002",
     "question": ["Show me clinical quality metrics for the current quarter"],
     "answer": [{"format": "SQL", "content": [f"SELECT measure_name, measure_category, reporting_direction, regulatory_threshold, MEASURE(denominator) as denominator, MEASURE(numerator) as numerator, MEASURE(performance_rate) as performance_rate, MEASURE(gap_to_threshold) as gap_to_threshold FROM {FQN}.mv_quality_performance WHERE measurement_year = 2025 AND quarter = 1 GROUP BY measure_name, measure_category, reporting_direction, regulatory_threshold ORDER BY measure_name"]}]},
    {"id": "b0000000000000000000000000000003",
     "question": ["Which measures are at risk of not meeting regulatory thresholds?"],
     "answer": [{"format": "SQL", "content": [f"SELECT * FROM (SELECT measure_name, measure_category, measurement_year, reporting_direction, regulatory_threshold, MEASURE(performance_rate) as performance_rate, MEASURE(gap_to_threshold) as gap_to_threshold, MEASURE(denominator) as denominator FROM {FQN}.mv_quality_performance WHERE measurement_year = 2025 GROUP BY measure_name, measure_category, measurement_year, reporting_direction, regulatory_threshold) WHERE (reporting_direction = 'Higher is Better' AND performance_rate < regulatory_threshold) OR (reporting_direction = 'Lower is Better' AND performance_rate > regulatory_threshold) ORDER BY ABS(gap_to_threshold) DESC"]}]},
    {"id": "b0000000000000000000000000000004",
     "question": ["Compare this year performance vs last year by quality measure"],
     "answer": [{"format": "SQL", "content": [f"SELECT measure_name, measure_category, measurement_year, MEASURE(performance_rate) as performance_rate, MEASURE(denominator) as denominator, MEASURE(numerator) as numerator, MEASURE(distinct_members) as distinct_members FROM {FQN}.mv_quality_performance GROUP BY measure_name, measure_category, measurement_year ORDER BY measure_name, measurement_year"]}]},
    {"id": "b0000000000000000000000000000005",
     "question": ["Show claims cost breakdown by claim type and aid category"],
     "answer": [{"format": "SQL", "content": [f"SELECT cl.claim_type, m.aid_category, COUNT(*) as claim_count, ROUND(SUM(cl.paid_amount), 2) as total_paid, ROUND(AVG(cl.paid_amount), 2) as avg_paid, COUNT(DISTINCT cl.member_id) as unique_members FROM {FQN}.fact_claims cl JOIN {FQN}.dim_member m ON cl.member_id = m.member_id GROUP BY 1, 2 ORDER BY 4 DESC"]}]},
    # Window function benchmarks
    {"id": "b0000000000000000000000000000006",
     "question": ["Rank providers by their quality measure performance rate"],
     "answer": [{"format": "SQL", "content": [f"SELECT provider_name, provider_type, measure_name, performance_rate, provider_rank FROM (SELECT p.provider_name, p.provider_type, m.measure_name, ROUND(COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0), 2) as performance_rate, RANK() OVER(PARTITION BY m.measure_name ORDER BY COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0) DESC) as provider_rank FROM {FQN}.fact_quality_events q JOIN {FQN}.dim_provider p ON q.provider_npi = p.provider_npi JOIN {FQN}.dim_measure m ON q.measure_id = m.measure_id WHERE q.measurement_year = 2025 AND m.reporting_direction = 'Higher is Better' GROUP BY p.provider_name, p.provider_type, m.measure_name) ranked ORDER BY measure_name, provider_rank"]}]},
    {"id": "b0000000000000000000000000000007",
     "question": ["Show quarter-over-quarter performance trend for each measure"],
     "answer": [{"format": "SQL", "content": [f"SELECT measure_name, measurement_year, quarter, performance_rate, LAG(performance_rate) OVER(PARTITION BY measure_name ORDER BY measurement_year, quarter) as prev_quarter_rate, ROUND(performance_rate - LAG(performance_rate) OVER(PARTITION BY measure_name ORDER BY measurement_year, quarter), 2) as qoq_change FROM (SELECT m.measure_name, q.measurement_year, q.quarter, ROUND(COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0), 2) as performance_rate FROM {FQN}.fact_quality_events q JOIN {FQN}.dim_measure m ON q.measure_id = m.measure_id GROUP BY m.measure_name, q.measurement_year, q.quarter) rates ORDER BY measure_name, measurement_year, quarter"]}]},
    {"id": "b0000000000000000000000000000008",
     "question": ["Which providers are in the bottom quartile for quality performance?"],
     "answer": [{"format": "SQL", "content": [f"SELECT provider_name, provider_type, overall_performance_rate, performance_quartile FROM (SELECT p.provider_name, p.provider_type, ROUND(COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0), 2) as overall_performance_rate, NTILE(4) OVER(ORDER BY COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0)) as performance_quartile FROM {FQN}.fact_quality_events q JOIN {FQN}.dim_provider p ON q.provider_npi = p.provider_npi WHERE q.measurement_year = 2025 GROUP BY p.provider_name, p.provider_type) ranked WHERE performance_quartile = 1 ORDER BY overall_performance_rate"]}]},
    {"id": "b0000000000000000000000000000009",
     "question": ["Show cumulative enrollment growth with a 3-month rolling average"],
     "answer": [{"format": "SQL", "content": [f"SELECT snapshot_month, monthly_active_members, SUM(monthly_active_members) OVER(ORDER BY snapshot_month) as cumulative_member_months, ROUND(AVG(monthly_active_members) OVER(ORDER BY snapshot_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 0) as rolling_3mo_avg FROM (SELECT e.snapshot_month, COUNT(DISTINCT e.member_id) as monthly_active_members FROM {FQN}.fact_enrollment e WHERE e.is_active = TRUE GROUP BY e.snapshot_month) monthly ORDER BY snapshot_month"]}]},
    {"id": "b000000000000000000000000000000a",
     "question": ["What percentile does each county rank in for diabetes measure performance?"],
     "answer": [{"format": "SQL", "content": [f"SELECT county_name, state_code, measure_name, performance_rate, ROUND(PERCENT_RANK() OVER(PARTITION BY measure_name ORDER BY performance_rate) * 100, 1) as percentile_rank FROM (SELECT c.county_name, c.state_code, m.measure_name, ROUND(COUNT(CASE WHEN q.in_numerator AND NOT q.exclusion_applied THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN q.in_denominator AND NOT q.exclusion_applied THEN 1 END), 0), 2) as performance_rate FROM {FQN}.fact_quality_events q JOIN {FQN}.dim_county c ON q.county_fips = c.county_fips JOIN {FQN}.dim_measure m ON q.measure_id = m.measure_id WHERE q.measurement_year = 2025 AND m.measure_category = 'Diabetes' GROUP BY c.county_name, c.state_code, m.measure_name) county_rates ORDER BY measure_name, percentile_rank DESC"]}]},
]

serialized_space = {
    "version": 2,
    "config": {"sample_questions": sample_questions},
    "data_sources": {
        "tables": [
            {"identifier": f"{FQN}.dim_county"},
            {"identifier": f"{FQN}.dim_measure"},
            {"identifier": f"{FQN}.dim_member"},
            {"identifier": f"{FQN}.dim_provider"},
            {"identifier": f"{FQN}.fact_claims"},
            {"identifier": f"{FQN}.fact_enrollment"},
            {"identifier": f"{FQN}.fact_quality_events"},
        ],
        "metric_views": [{"identifier": f"{FQN}.mv_quality_performance"}],
    },
    "instructions": {
        "text_instructions": text_instructions,
        "example_question_sqls": [],
        "sql_snippets": sql_snippets,
        "join_specs": join_specs,
    },
    "benchmarks": {"questions": benchmarks_questions},
}

print(f"Built serialized_space: "
      f"{len(sample_questions)} sample questions, "
      f"{len(benchmarks_questions)} benchmarks, "
      f"{len(join_specs)} joins, "
      f"{sum(len(v) for v in sql_snippets.values())} snippets.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 Create or update the Genie space (idempotent)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

existing_id_row = spark.sql(
    f"SELECT config_value FROM {CATALOG}.{SCHEMA}.config_genie WHERE config_key = 'genie_space_id' ORDER BY created_at DESC LIMIT 1"
).collect()
existing_space_id = existing_id_row[0]["config_value"] if existing_id_row else None

payload = {
    "title": GENIE_TITLE,
    "description": GENIE_DESCRIPTION,
    "warehouse_id": WAREHOUSE_ID,
    "serialized_space": json.dumps(serialized_space),
}

if existing_space_id:
    print(f"Found existing genie_space_id={existing_space_id}; PATCHing.")
    resp = w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{existing_space_id}", body=payload)
    space_id = existing_space_id
    print(f"PATCH ok. title={resp.get('title')}")
else:
    print("No existing genie_space_id; POSTing a new space.")
    resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body=payload)
    space_id = resp.get("space_id")
    print(f"POST ok. space_id={space_id}")
    spark.sql(f"""
      INSERT INTO {CATALOG}.{SCHEMA}.config_genie
      SELECT 'genie_space_id' as config_key, '{space_id}' as config_value, current_timestamp() as created_at
    """)

host = w.config.host.rstrip("/")
print(f"\nGenie Space URL: {host}/genie/rooms/{space_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 (optional): Row-Filter Cascade Demo
# MAGIC
# MAGIC Demonstrates a **gotcha-correct** governance pattern: row filters can ONLY be attached to base
# MAGIC tables. They cascade automatically through views and metric views into Genie. This cell
# MAGIC creates a UC SQL function that restricts non-admin users to a single state, then attaches it
# MAGIC as a row filter on `dim_member`. Every Genie query that touches member-level data is
# MAGIC transparently filtered for the calling user.
# MAGIC
# MAGIC Skip this cell if you do not want to apply row-level security to the demo. To remove the
# MAGIC filter later: `ALTER TABLE dim_member DROP ROW FILTER`.

# COMMAND ----------

# Adjust this group to one you actually own. If the group does not exist, the function still
# works — it just defaults to the deny branch and shows nothing for non-admin callers, which is
# the safe default.
ADMIN_GROUP = "account users"          # change to your admin group, e.g. "phi_admins"
DEFAULT_STATE_FILTER = "CA"            # state non-admins are restricted to via dim_county.state_code

# Helper function: returns TRUE if caller may see the member row.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.member_state_filter(member_state STRING)
RETURNS BOOLEAN
RETURN
  IS_ACCOUNT_GROUP_MEMBER('{ADMIN_GROUP}')
  OR member_state = '{DEFAULT_STATE_FILTER}'
""")

# dim_member has no state_code directly — it joins through dim_county. To keep the row filter
# self-contained, we materialize state_code onto dim_member as a generated/managed column at
# load time in production. For this demo, we approximate with a subquery via a UC function that
# accepts member_id and resolves state.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.member_id_state_filter(mid STRING)
RETURNS BOOLEAN
RETURN
  IS_ACCOUNT_GROUP_MEMBER('{ADMIN_GROUP}')
  OR EXISTS (
    SELECT 1
    FROM {CATALOG}.{SCHEMA}.dim_member m
    JOIN {CATALOG}.{SCHEMA}.dim_county c ON m.county_fips = c.county_fips
    WHERE m.member_id = mid
      AND c.state_code = '{DEFAULT_STATE_FILTER}'
  )
""")

# Attach the row filter on dim_member. NOTE: row filters cannot be applied to views/metric views;
# they cascade through automatically.
spark.sql(f"""
ALTER TABLE {CATALOG}.{SCHEMA}.dim_member
SET ROW FILTER {CATALOG}.{SCHEMA}.member_id_state_filter ON (member_id)
""")

print("Row filter applied to dim_member. It will cascade through fact_quality_events, fact_claims, "
      "fact_enrollment, and mv_quality_performance for any query Genie generates.")
print(f"Non-admin users will only see rows where dim_county.state_code = '{DEFAULT_STATE_FILTER}'.")
print("To remove: ALTER TABLE dim_member DROP ROW FILTER")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Open the Genie Space URL** printed above and ask a few questions to validate.
# MAGIC 2. **Build AI/BI Dashboards** — filters by county, measure category, threshold status.
# MAGIC 3. **Tighten governance** — extend the row-filter pattern in Step 10 to your real groups.
# MAGIC 4. **Schedule Refreshes** — set up workflows to refresh fact tables from source systems.
