# Databricks notebook source
# MAGIC %md
# MAGIC # Medicaid Clinical Quality Measures - Data Warehouse Setup
# MAGIC
# MAGIC This notebook creates a complete Medicaid clinical data warehouse with:
# MAGIC - **7 Delta tables** (4 dimensions + 3 fact tables) with realistic synthetic data
# MAGIC - **1 YoY comparison view** for quality measure performance tracking
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
# MAGIC - `v_yoy_quality_performance` — YoY comparison view

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
# MAGIC ## Step 6: Create Year-over-Year Comparison View

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_yoy_quality_performance
COMMENT 'Year-over-year quality measure performance comparison. Precomputed rates with threshold status and trend direction.'
AS
WITH annual_perf AS (
    SELECT
        qe.measure_id,
        qe.measurement_year,
        qe.county_fips,
        dm.measure_name,
        dm.measure_category,
        dm.reporting_direction,
        dm.regulatory_threshold,
        COUNT(CASE WHEN qe.in_denominator AND NOT qe.exclusion_applied THEN 1 END) AS denominator,
        COUNT(CASE WHEN qe.in_numerator AND NOT qe.exclusion_applied THEN 1 END) AS numerator,
        ROUND(
          COUNT(CASE WHEN qe.in_numerator AND NOT qe.exclusion_applied THEN 1 END) * 100.0
          / NULLIF(COUNT(CASE WHEN qe.in_denominator AND NOT qe.exclusion_applied THEN 1 END), 0), 2
        ) AS performance_rate
    FROM {CATALOG}.{SCHEMA}.fact_quality_events qe
    JOIN {CATALOG}.{SCHEMA}.dim_measure dm USING (measure_id)
    GROUP BY 1,2,3,4,5,6,7
)
SELECT
    cy.measure_id, cy.measure_name, cy.measure_category, cy.reporting_direction,
    cy.regulatory_threshold, cy.county_fips,
    py.measurement_year AS prior_year, cy.measurement_year AS current_year,
    py.denominator AS prior_denom, cy.denominator AS current_denom,
    py.performance_rate AS prior_year_rate, cy.performance_rate AS current_year_rate,
    ROUND(cy.performance_rate - COALESCE(py.performance_rate, 0), 2) AS rate_delta,
    CASE
        WHEN cy.reporting_direction = 'Higher is Better' AND cy.performance_rate >= cy.regulatory_threshold THEN 'Met'
        WHEN cy.reporting_direction = 'Lower is Better' AND cy.performance_rate <= cy.regulatory_threshold THEN 'Met'
        ELSE 'At Risk'
    END AS threshold_status,
    CASE
        WHEN py.performance_rate IS NULL THEN 'New'
        WHEN cy.reporting_direction = 'Higher is Better' AND cy.performance_rate > py.performance_rate THEN 'Improved'
        WHEN cy.reporting_direction = 'Higher is Better' AND cy.performance_rate < py.performance_rate THEN 'Declined'
        WHEN cy.reporting_direction = 'Lower is Better' AND cy.performance_rate < py.performance_rate THEN 'Improved'
        WHEN cy.reporting_direction = 'Lower is Better' AND cy.performance_rate > py.performance_rate THEN 'Declined'
        ELSE 'Stable'
    END AS trend_direction
FROM annual_perf cy
LEFT JOIN annual_perf py
    ON cy.measure_id = py.measure_id
   AND cy.county_fips = py.county_fips
   AND py.measurement_year = cy.measurement_year - 1
""")
print("v_yoy_quality_performance view created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Validate Data

# COMMAND ----------

tables = ['dim_county','dim_provider','dim_member','dim_measure','fact_quality_events','fact_enrollment','fact_claims','v_yoy_quality_performance']
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{t}").collect()[0][0]
    print(f"{t}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Sample Queries
# MAGIC
# MAGIC These queries demonstrate the key analytics this data warehouse supports.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enrollment by county (top 10)
# MAGIC SELECT c.county_name, c.state_code, COUNT(DISTINCT e.member_id) as enrolled_members
# MAGIC FROM ${CATALOG}.${SCHEMA}.fact_enrollment e
# MAGIC JOIN ${CATALOG}.${SCHEMA}.dim_county c ON e.county_fips = c.county_fips
# MAGIC WHERE e.is_active = TRUE
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 3 DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- At-risk measures
# MAGIC SELECT measure_name, measure_category, current_year_rate, regulatory_threshold, rate_delta, threshold_status
# MAGIC FROM ${CATALOG}.${SCHEMA}.v_yoy_quality_performance
# MAGIC WHERE threshold_status = 'At Risk' AND current_year = 2025
# MAGIC ORDER BY ABS(rate_delta) DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Year-over-year comparison
# MAGIC SELECT measure_name, measure_category, prior_year_rate, current_year_rate, rate_delta, trend_direction
# MAGIC FROM ${CATALOG}.${SCHEMA}.v_yoy_quality_performance
# MAGIC WHERE current_year = 2025
# MAGIC ORDER BY ABS(rate_delta) DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Create a Genie Space** — Add the tables above to an AI/BI Genie space for natural language querying
# MAGIC 2. **Build AI/BI Dashboards** — Create dashboards with filters by county, measure category, and threshold status
# MAGIC 3. **Apply Row-Level Security** — Use the PHI/PII tags to create access policies
# MAGIC 4. **Schedule Refreshes** — Set up workflows to refresh fact tables from source systems
