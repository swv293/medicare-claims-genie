CREATE OR REPLACE VIEW serverless_stable_swv01_catalog.medicaid_clinical.v_yoy_quality_performance
COMMENT 'Year-over-year quality measure performance comparison view. Computes performance rates by measure, county, and year with trend analysis and regulatory threshold status. Join of fact_quality_events and dim_measure.'
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
        COUNT(CASE WHEN qe.in_denominator AND NOT qe.exclusion_applied THEN 1 END)   AS denominator,
        COUNT(CASE WHEN qe.in_numerator  AND NOT qe.exclusion_applied THEN 1 END)   AS numerator,
        ROUND(
          COUNT(CASE WHEN qe.in_numerator  AND NOT qe.exclusion_applied THEN 1 END) * 100.0
          / NULLIF(COUNT(CASE WHEN qe.in_denominator AND NOT qe.exclusion_applied THEN 1 END), 0),
          2
        ) AS performance_rate
    FROM serverless_stable_swv01_catalog.medicaid_clinical.fact_quality_events qe
    JOIN serverless_stable_swv01_catalog.medicaid_clinical.dim_measure dm USING (measure_id)
    GROUP BY 1,2,3,4,5,6,7
)
SELECT
    cy.measure_id,
    cy.measure_name,
    cy.measure_category,
    cy.reporting_direction,
    cy.regulatory_threshold,
    cy.county_fips,
    py.measurement_year    AS prior_year,
    cy.measurement_year    AS current_year,
    py.denominator         AS prior_denom,
    cy.denominator         AS current_denom,
    py.performance_rate    AS prior_year_rate,
    cy.performance_rate    AS current_year_rate,
    ROUND(cy.performance_rate - COALESCE(py.performance_rate, 0), 2)   AS rate_delta,
    CASE
        WHEN cy.reporting_direction = 'Higher is Better'
             AND cy.performance_rate >= cy.regulatory_threshold THEN 'Met'
        WHEN cy.reporting_direction = 'Lower is Better'
             AND cy.performance_rate <= cy.regulatory_threshold THEN 'Met'
        ELSE 'At Risk'
    END AS threshold_status,
    CASE
        WHEN py.performance_rate IS NULL THEN 'New'
        WHEN cy.reporting_direction = 'Higher is Better'
             AND cy.performance_rate > py.performance_rate THEN 'Improved'
        WHEN cy.reporting_direction = 'Higher is Better'
             AND cy.performance_rate < py.performance_rate THEN 'Declined'
        WHEN cy.reporting_direction = 'Lower is Better'
             AND cy.performance_rate < py.performance_rate THEN 'Improved'
        WHEN cy.reporting_direction = 'Lower is Better'
             AND cy.performance_rate > py.performance_rate THEN 'Declined'
        ELSE 'Stable'
    END AS trend_direction
FROM annual_perf cy
LEFT JOIN annual_perf py
    ON cy.measure_id = py.measure_id
   AND cy.county_fips = py.county_fips
   AND py.measurement_year = cy.measurement_year - 1;
