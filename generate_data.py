"""
Medicaid Clinical Data Generator
Generates realistic synthetic data for all dimension and fact tables.
Outputs SQL INSERT statements for Databricks Delta tables.
"""
import random
import json
from datetime import date, timedelta
from collections import defaultdict

random.seed(42)

CATALOG = "serverless_stable_swv01_catalog"
SCHEMA = "medicaid_clinical"

# ─── Helper functions ───
def rdate(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def sql_val(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, date):
        return f"'{v.isoformat()}'"
    if isinstance(v, list):
        return "ARRAY(" + ",".join(f"'{x}'" for x in v) + ")"
    return f"'{str(v).replace(chr(39), chr(39)+chr(39))}'"

# ─── 1. dim_county (2500 rows) ───
STATES = {
    'CA': ('California', ['Los Angeles','San Diego','San Francisco','Sacramento','Fresno','Kern','Riverside','San Bernardino','Orange','Alameda','Santa Clara','Contra Costa','Ventura','San Joaquin','Stanislaus','Tulare','Monterey','Solano','Sonoma','Marin','Placer','San Mateo','Butte','Shasta','Yolo','Humboldt','Lake','Merced','Madera','Kings']),
    'TX': ('Texas', ['Harris','Dallas','Tarrant','Bexar','Travis','Collin','Denton','Hidalgo','El Paso','Fort Bend','Williamson','Montgomery','Cameron','Nueces','Brazoria','Bell','Lubbock','Webb','McLennan','Galveston','Smith','Midland','Ector','Hays','Johnson','Ellis','Wichita','Randall','Tom Green','Grayson']),
    'NY': ('New York', ['New York','Kings','Queens','Bronx','Richmond','Nassau','Suffolk','Westchester','Erie','Monroe','Onondaga','Albany','Dutchess','Orange','Rockland','Saratoga','Oneida','Broome','Niagara','Rensselaer','Schenectady','Chautauqua','Tompkins','Ulster','St. Lawrence','Jefferson','Chemung','Cattaraugus','Steuben','Otsego']),
    'FL': ('Florida', ['Miami-Dade','Broward','Palm Beach','Hillsborough','Orange','Pinellas','Duval','Lee','Polk','Brevard','Volusia','Seminole','Pasco','Sarasota','Manatee','Osceola','Marion','Collier','Escambia','Leon','Alachua','St. Lucie','Bay','Okaloosa','Santa Rosa','Clay','St. Johns','Lake','Hernando','Indian River']),
    'PA': ('Pennsylvania', ['Philadelphia','Allegheny','Montgomery','Bucks','Delaware','Lancaster','Chester','York','Berks','Lehigh','Northampton','Luzerne','Dauphin','Erie','Cumberland','Lackawanna','Westmoreland','Monroe','Beaver','Washington','Butler','Centre','Schuylkill','Cambria','Blair','Lebanon','Lycoming','Franklin','Fayette','Adams']),
    'IL': ('Illinois', ['Cook','DuPage','Lake','Will','Kane','McHenry','Winnebago','St. Clair','Madison','Champaign','Sangamon','Peoria','McLean','Rock Island','Tazewell','Kankakee','DeKalb','Macon','LaSalle','Vermilion','Adams','Kendall','Grundy','Livingston','Whiteside','Ogle','Lee','Bureau','Knox','Henry']),
    'OH': ('Ohio', ['Cuyahoga','Franklin','Hamilton','Summit','Montgomery','Lucas','Stark','Butler','Lorain','Warren','Lake','Mahoning','Clermont','Delaware','Medina','Licking','Fairfield','Portage','Trumbull','Wood','Richland','Allen','Wayne','Columbiana','Geauga','Tuscarawas','Ashtabula','Hancock','Ross','Miami']),
    'GA': ('Georgia', ['Fulton','Gwinnett','Cobb','DeKalb','Chatham','Richmond','Clayton','Cherokee','Henry','Forsyth','Hall','Bibb','Muscogee','Columbia','Houston','Douglas','Paulding','Whitfield','Dougherty','Floyd','Lowndes','Bartow','Glynn','Carroll','Bulloch','Coweta','Fayette','Clarke','Liberty','Troup']),
    'MI': ('Michigan', ['Wayne','Oakland','Macomb','Kent','Genesee','Washtenaw','Ingham','Ottawa','Kalamazoo','Saginaw','Muskegon','St. Clair','Livingston','Monroe','Jackson','Berrien','Calhoun','Allegan','Bay','Eaton','Midland','Isabella','Lenawee','Shiawassee','Gratiot','Tuscola','Clare','Mecosta','Montcalm','Ionia']),
    'NC': ('North Carolina', ['Mecklenburg','Wake','Guilford','Forsyth','Cumberland','Durham','Buncombe','New Hanover','Cabarrus','Gaston','Union','Onslow','Johnston','Pitt','Davidson','Catawba','Rowan','Alamance','Randolph','Wayne','Robeson','Iredell','Craven','Nash','Harnett','Henderson','Lenoir','Lee','Moore','Chatham']),
}

REGIONS = ['North', 'South', 'East', 'West', 'Central', 'Northeast', 'Southeast', 'Northwest', 'Southwest', 'Metro']
RURAL_CLASSES = ['Metropolitan', 'Micropolitan', 'Rural', 'Frontier']

counties = []
fips_counter = 1000
for state_code, (state_name, county_names) in STATES.items():
    for cname in county_names:
        fips = str(fips_counter).zfill(5)
        fips_counter += 1
        counties.append({
            'county_fips': fips,
            'county_name': cname,
            'region': random.choice(REGIONS),
            'urban_rural_class': random.choices(RURAL_CLASSES, weights=[50,25,20,5])[0],
            'state_code': state_code
        })

# Pad to 2500 with additional synthetic counties
extra_states = ['WA','OR','AZ','NV','CO','NM','UT','ID','MT','WY','NE','KS','OK','AR','LA','MS','AL','TN','KY','WV','VA','MD','NJ','CT','MA','RI','NH','VT','ME','SC','MN','IA','MO','WI','IN','SD','ND','HI','AK','DC','DE']
while len(counties) < 2500:
    st = random.choice(extra_states)
    fips = str(fips_counter).zfill(5)
    fips_counter += 1
    counties.append({
        'county_fips': fips,
        'county_name': f"{st}-County-{len(counties)}",
        'region': random.choice(REGIONS),
        'urban_rural_class': random.choices(RURAL_CLASSES, weights=[50,25,20,5])[0],
        'state_code': st
    })

county_fips_list = [c['county_fips'] for c in counties]

# ─── 2. dim_provider (500 rows) ───
PROVIDER_TYPES = ['PCP', 'Specialist', 'FQHC', 'BH', 'OB/GYN', 'Pediatrics', 'Urgent Care', 'Hospital']
SPECIALTIES = {
    'PCP': '207Q00000X', 'Specialist': '207R00000X', 'FQHC': '261QF0400X',
    'BH': '103T00000X', 'OB/GYN': '207V00000X', 'Pediatrics': '208000000X',
    'Urgent Care': '261QU0200X', 'Hospital': '282N00000X'
}
FIRST_NAMES = ['James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen','Christopher','Lisa','Daniel','Nancy','Matthew','Betty','Anthony','Margaret','Mark','Sandra']
LAST_NAMES = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin','Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson']

providers = []
for i in range(500):
    ptype = random.choice(PROVIDER_TYPES)
    providers.append({
        'provider_npi': str(1000000000 + i),
        'provider_name': f"Dr. {random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
        'provider_type': ptype,
        'specialty_code': SPECIALTIES.get(ptype, '207Q00000X'),
        'county_fips': random.choice(county_fips_list[:300]),
        'accepting_medicaid': random.random() < 0.85
    })

# ─── 3. dim_member (1000 rows) ───
AID_CATS = ['TANF', 'SSI', 'CHIP', 'Expansion Adult']
RACES = ['White', 'Black or African American', 'Hispanic or Latino', 'Asian', 'American Indian/Alaska Native', 'Native Hawaiian/Pacific Islander', 'Two or More Races']
CHRONIC_CONDITIONS = ['Diabetes', 'Hypertension', 'Asthma', 'COPD', 'CHF', 'Depression', 'Anxiety', 'Obesity', 'CKD', 'Substance Use Disorder']

members = []
for i in range(1000):
    dob = rdate(date(1945, 1, 1), date(2022, 12, 31))
    enroll_start = rdate(date(2020, 1, 1), date(2024, 6, 1))
    enroll_end = None if random.random() < 0.7 else rdate(enroll_start + timedelta(days=90), date(2025, 12, 31))
    num_chronic = random.choices([0,1,2,3,4], weights=[30,30,20,15,5])[0]
    members.append({
        'member_id': f"MED{str(i+1).zfill(7)}",
        'date_of_birth': dob,
        'gender': random.choices(['M','F','Other'], weights=[48,50,2])[0],
        'race_ethnicity': random.choice(RACES),
        'county_fips': random.choice(county_fips_list[:300]),
        'zip_code': str(random.randint(10000, 99999)),
        'aid_category': random.choices(AID_CATS, weights=[35,20,25,20])[0],
        'smi_flag': random.random() < 0.08,
        'chronic_condition_flags': random.sample(CHRONIC_CONDITIONS, num_chronic) if num_chronic > 0 else [],
        'enrollment_start_dt': enroll_start,
        'enrollment_end_dt': enroll_end
    })

# ─── 4. dim_measure (18 seeded rows) ───
measures = [
    {'measure_id':'CDC-HbA1c','measure_name':'Glycemic Status Assessment for Patients with Diabetes','measure_category':'Diabetes','reporting_standard':'HEDIS','numerator_definition':'Patients with diabetes whose most recent HbA1c level is >9.0%','denominator_definition':'Patients 18-75 with diabetes (Type 1 or Type 2)','exclusion_definition':'Patients in hospice or with ESRD','age_range':'18-75','measurement_year':2025,'reporting_direction':'Lower is Better','regulatory_threshold':25.0,'high_priority_flag':True,'star_rating_flag':True,'data_source':'Hybrid'},
    {'measure_id':'CDC-EYE','measure_name':'Eye Exam for Patients with Diabetes','measure_category':'Diabetes','reporting_standard':'HEDIS','numerator_definition':'Patients who had a retinal eye exam by an eye care professional','denominator_definition':'Patients 18-75 with diabetes (Type 1 or Type 2)','exclusion_definition':'Patients in hospice','age_range':'18-75','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':55.0,'high_priority_flag':True,'star_rating_flag':True,'data_source':'Hybrid'},
    {'measure_id':'CDC-NEP','measure_name':'Kidney Health Evaluation for Patients with Diabetes','measure_category':'Diabetes','reporting_standard':'HEDIS','numerator_definition':'Patients with both urine albumin-creatinine ratio and eGFR lab tests','denominator_definition':'Patients 18-85 with diabetes','exclusion_definition':'Patients in hospice or with ESRD on dialysis','age_range':'18-85','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':40.0,'high_priority_flag':False,'star_rating_flag':False,'data_source':'Admin'},
    {'measure_id':'CBP','measure_name':'Controlling High Blood Pressure','measure_category':'Cardiovascular','reporting_standard':'HEDIS','numerator_definition':'Patients whose most recent BP is adequately controlled (<140/90)','denominator_definition':'Patients 18-85 diagnosed with hypertension','exclusion_definition':'Patients in hospice, with ESRD, or pregnancy','age_range':'18-85','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':60.0,'high_priority_flag':True,'star_rating_flag':True,'data_source':'Hybrid'},
    {'measure_id':'BCS','measure_name':'Breast Cancer Screening','measure_category':'Preventive','reporting_standard':'HEDIS','numerator_definition':'Women with a mammogram during the measurement year or prior year','denominator_definition':'Women 50-74','exclusion_definition':'Women with bilateral mastectomy','age_range':'50-74','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':55.0,'high_priority_flag':True,'star_rating_flag':True,'data_source':'Admin'},
    {'measure_id':'CCS','measure_name':'Cervical Cancer Screening','measure_category':'Preventive','reporting_standard':'HEDIS','numerator_definition':'Women screened for cervical cancer (Pap within 3 years or HPV within 5 years)','denominator_definition':'Women 21-64','exclusion_definition':'Women with hysterectomy with no residual cervix','age_range':'21-64','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':55.0,'high_priority_flag':True,'star_rating_flag':True,'data_source':'Hybrid'},
    {'measure_id':'COL','measure_name':'Colorectal Cancer Screening','measure_category':'Preventive','reporting_standard':'HEDIS','numerator_definition':'Patients with appropriate colorectal cancer screening','denominator_definition':'Adults 45-75','exclusion_definition':'Patients with colorectal cancer or total colectomy','age_range':'45-75','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':50.0,'high_priority_flag':True,'star_rating_flag':True,'data_source':'Admin'},
    {'measure_id':'W34','measure_name':'Well-Child Visits (3rd-6th Year of Life)','measure_category':'Child Health','reporting_standard':'CMS Child Core Set','numerator_definition':'Children with at least one well-child visit during the measurement year','denominator_definition':'Children aged 3-6','exclusion_definition':'None','age_range':'3-6','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':70.0,'high_priority_flag':True,'star_rating_flag':False,'data_source':'Admin'},
    {'measure_id':'CIS','measure_name':'Childhood Immunization Status','measure_category':'Immunization','reporting_standard':'CMS Child Core Set','numerator_definition':'Children with all recommended immunizations by 2nd birthday','denominator_definition':'Children who turned 2 during the measurement year','exclusion_definition':'Children in hospice','age_range':'2','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':65.0,'high_priority_flag':True,'star_rating_flag':False,'data_source':'Admin'},
    {'measure_id':'FUH','measure_name':'Follow-Up After Hospitalization for Mental Illness','measure_category':'Behavioral Health','reporting_standard':'HEDIS','numerator_definition':'Patients with follow-up visit within 7 days of discharge','denominator_definition':'Patients 6+ discharged from inpatient mental health facility','exclusion_definition':'Patients who died during stay','age_range':'6+','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':45.0,'high_priority_flag':True,'star_rating_flag':True,'data_source':'Admin'},
    {'measure_id':'FUM','measure_name':'Follow-Up After ED Visit for Mental Illness','measure_category':'Behavioral Health','reporting_standard':'CMS Adult Core Set','numerator_definition':'Patients with follow-up visit within 7 days of ED visit','denominator_definition':'Patients 6+ with ED visit for mental illness','exclusion_definition':'Patients who were admitted directly from ED','age_range':'6+','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':40.0,'high_priority_flag':False,'star_rating_flag':False,'data_source':'Admin'},
    {'measure_id':'AMR','measure_name':'Asthma Medication Ratio','measure_category':'Respiratory','reporting_standard':'HEDIS','numerator_definition':'Patients with ratio of controller medications to total asthma medications >= 0.50','denominator_definition':'Patients 5-64 with persistent asthma','exclusion_definition':'Patients with COPD, emphysema, or cystic fibrosis','age_range':'5-64','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':60.0,'high_priority_flag':True,'star_rating_flag':True,'data_source':'Admin'},
    {'measure_id':'AMM','measure_name':'Antidepressant Medication Management','measure_category':'Behavioral Health','reporting_standard':'HEDIS','numerator_definition':'Patients who remained on antidepressant medication for at least 84 days (acute) or 180 days (continuation)','denominator_definition':'Patients 18+ with new episode of major depression and antidepressant dispensing','exclusion_definition':'Patients with prior antidepressant use in 105 days before index','age_range':'18+','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':50.0,'high_priority_flag':False,'star_rating_flag':True,'data_source':'Admin'},
    {'measure_id':'PPC','measure_name':'Prenatal and Postpartum Care','measure_category':'Maternal Health','reporting_standard':'HEDIS','numerator_definition':'Women with prenatal visit in first trimester or within 42 days of enrollment, and postpartum visit 7-84 days after delivery','denominator_definition':'Women with live birth delivery','exclusion_definition':'None','age_range':'15-44','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':65.0,'high_priority_flag':True,'star_rating_flag':False,'data_source':'Hybrid'},
    {'measure_id':'WCC-BMI','measure_name':'Weight Assessment and Counseling - BMI Percentile','measure_category':'Nutrition/Obesity','reporting_standard':'HEDIS','numerator_definition':'Children with BMI percentile documented during the measurement year','denominator_definition':'Children 3-17','exclusion_definition':'Children with pregnancy','age_range':'3-17','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':55.0,'high_priority_flag':False,'star_rating_flag':False,'data_source':'Hybrid'},
    {'measure_id':'AIS-E','measure_name':'Adult Immunization Status','measure_category':'Preventive','reporting_standard':'HEDIS','numerator_definition':'Adults with up-to-date immunization status per ACIP recommendations','denominator_definition':'Adults 19+','exclusion_definition':'Patients in hospice','age_range':'19+','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':40.0,'high_priority_flag':False,'star_rating_flag':False,'data_source':'Admin'},
    {'measure_id':'SUD-LOT','measure_name':'Initiation and Engagement of SUD Treatment','measure_category':'Behavioral Health','reporting_standard':'CMS Adult Core Set','numerator_definition':'Patients who initiated SUD treatment within 14 days and had 2+ additional services within 34 days','denominator_definition':'Patients 13+ with new SUD diagnosis','exclusion_definition':'Patients in active treatment for SUD in prior 60 days','age_range':'13+','measurement_year':2025,'reporting_direction':'Higher is Better','regulatory_threshold':40.0,'high_priority_flag':True,'star_rating_flag':False,'data_source':'Admin'},
    {'measure_id':'PCR','measure_name':'Plan All-Cause Readmissions','measure_category':'Utilization','reporting_standard':'HEDIS','numerator_definition':'Patients with unplanned readmission within 30 days of index discharge','denominator_definition':'Patients 18+ discharged from acute inpatient stay','exclusion_definition':'Planned readmissions, patients who died, AMA discharges','age_range':'18+','measurement_year':2025,'reporting_direction':'Lower is Better','regulatory_threshold':15.0,'high_priority_flag':True,'star_rating_flag':True,'data_source':'Admin'},
]

# ─── 5. fact_quality_events (10000 rows) ───
measure_ids = [m['measure_id'] for m in measures]
quality_events = []
for i in range(10000):
    my = random.choice([2024, 2025])
    quarter = random.randint(1, 4)
    m_id = random.choice(measure_ids)
    member = random.choice(members)
    in_denom = random.random() < 0.85
    in_numer = random.random() < 0.65 if in_denom else False
    excl = random.random() < 0.05 if in_denom else False
    q_start = date(my, (quarter-1)*3+1, 1)
    svc_date = rdate(q_start, q_start + timedelta(days=89))
    quality_events.append({
        'event_id': i + 1,
        'member_id': member['member_id'],
        'measure_id': m_id,
        'measurement_year': my,
        'quarter': quarter,
        'in_denominator': in_denom,
        'in_numerator': in_numer,
        'exclusion_applied': excl,
        'service_date': svc_date,
        'provider_npi': random.choice(providers)['provider_npi'],
        'data_source': random.choice(['Admin', 'EHR', 'Hybrid']),
        'county_fips': member['county_fips']
    })

# ─── 6. fact_enrollment (3000 rows) ───
enrollments = []
for i in range(3000):
    member = random.choice(members)
    yr = random.choice([2024, 2025])
    mo = random.randint(1, 12)
    enrollments.append({
        'snapshot_month': date(yr, mo, 1),
        'member_id': member['member_id'],
        'aid_category': member['aid_category'],
        'county_fips': member['county_fips'],
        'plan_id': random.choice(['MCO-BlueCross', 'MCO-Aetna', 'MCO-UHC', 'MCO-Centene', 'MCO-Molina', 'FFS']),
        'is_active': random.random() < 0.9
    })

# ─── 7. fact_claims (10000 rows) ───
DX_CODES = ['E11.9','E11.65','I10','J45.20','J44.1','F32.1','F41.1','E66.01','N18.3','F10.20','Z23','Z00.129','Z12.11','Z12.31','Z01.00','O80','G47.33','M54.5','K21.0','R10.9']
PROC_CODES = ['99213','99214','99215','99395','99396','83036','81001','85025','80053','36415','90471','90686','77067','88175','45378','99381','59400','96127','90837','99243']
CLAIM_TYPES = ['IP', 'OP', 'Prof', 'Rx']
REV_CODES = ['0100','0110','0120','0250','0260','0270','0300','0320','0450','0510']

claims = []
for i in range(10000):
    member = random.choice(members)
    svc_date = rdate(date(2024, 1, 1), date(2025, 12, 31))
    num_dx = random.randint(1, 4)
    num_proc = random.randint(1, 3)
    claims.append({
        'claim_id': f"CLM{str(i+1).zfill(8)}",
        'member_id': member['member_id'],
        'provider_npi': random.choice(providers)['provider_npi'],
        'service_date': svc_date,
        'claim_type': random.choices(CLAIM_TYPES, weights=[15,35,40,10])[0],
        'dx_codes': random.sample(DX_CODES, num_dx),
        'proc_codes': random.sample(PROC_CODES, num_proc),
        'revenue_code': random.choice(REV_CODES),
        'paid_amount': round(random.uniform(15.0, 25000.0), 2),
        'measurement_year': svc_date.year
    })


# ─── Generate SQL ───
def batch_insert(table, rows, columns, batch_size=200):
    """Generate INSERT statements in batches."""
    stmts = []
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start+batch_size]
        values = []
        for r in batch:
            vals = ", ".join(sql_val(r[c]) for c in columns)
            values.append(f"({vals})")
        cols_str = ", ".join(columns)
        stmt = f"INSERT INTO {CATALOG}.{SCHEMA}.{table} ({cols_str}) VALUES\n" + ",\n".join(values)
        stmts.append(stmt)
    return stmts

# Output all SQL
output = []

# dim_county
output.append("-- dim_county inserts")
county_cols = ['county_fips','county_name','region','urban_rural_class','state_code']
output.extend(batch_insert('dim_county', counties, county_cols, 250))

# dim_provider
output.append("-- dim_provider inserts")
prov_cols = ['provider_npi','provider_name','provider_type','specialty_code','county_fips','accepting_medicaid']
output.extend(batch_insert('dim_provider', providers, prov_cols, 250))

# dim_member
output.append("-- dim_member inserts")
member_cols = ['member_id','date_of_birth','gender','race_ethnicity','county_fips','zip_code','aid_category','smi_flag','chronic_condition_flags','enrollment_start_dt','enrollment_end_dt']
output.extend(batch_insert('dim_member', members, member_cols, 100))

# dim_measure
output.append("-- dim_measure inserts")
measure_cols = ['measure_id','measure_name','measure_category','reporting_standard','numerator_definition','denominator_definition','exclusion_definition','age_range','measurement_year','reporting_direction','regulatory_threshold','high_priority_flag','star_rating_flag','data_source']
output.extend(batch_insert('dim_measure', measures, measure_cols, 50))

# fact_quality_events
output.append("-- fact_quality_events inserts")
qe_cols = ['event_id','member_id','measure_id','measurement_year','quarter','in_denominator','in_numerator','exclusion_applied','service_date','provider_npi','data_source','county_fips']
output.extend(batch_insert('fact_quality_events', quality_events, qe_cols, 200))

# fact_enrollment
output.append("-- fact_enrollment inserts")
enr_cols = ['snapshot_month','member_id','aid_category','county_fips','plan_id','is_active']
output.extend(batch_insert('fact_enrollment', enrollments, enr_cols, 200))

# fact_claims
output.append("-- fact_claims inserts")
clm_cols = ['claim_id','member_id','provider_npi','service_date','claim_type','dx_codes','proc_codes','revenue_code','paid_amount','measurement_year']
output.extend(batch_insert('fact_claims', claims, clm_cols, 200))

# Write to file
with open('/Users/swami.venkatesh/medicare_claims_genie/insert_data.sql', 'w') as f:
    for stmt in output:
        f.write(stmt + ";\n\n")

print(f"Generated {len(output)} SQL statements")
print(f"  dim_county: {len(counties)} rows")
print(f"  dim_provider: {len(providers)} rows")
print(f"  dim_member: {len(members)} rows")
print(f"  dim_measure: {len(measures)} rows")
print(f"  fact_quality_events: {len(quality_events)} rows")
print(f"  fact_enrollment: {len(enrollments)} rows")
print(f"  fact_claims: {len(claims)} rows")
