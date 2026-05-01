--Before I write any staging model, I run a profiling query like this. It surfaces sentinel values, NULL distributions, the actual domain of categorical columns. Saves a lot of debugging later.

SELECT
    'Total rows' AS metric, CAST(COUNT(*) AS STRING) AS value FROM workspace.hrrp_raw.raw_admissions
UNION ALL
SELECT
    'Distinct facility_ids',
    CAST(COUNT(DISTINCT `Facility ID`) AS STRING)
FROM workspace.hrrp_raw.raw_admissions
UNION ALL
SELECT
    'Hospital ownership categories (raw)',
    CAST(COUNT(DISTINCT `Hospital Ownership`) AS STRING)
FROM workspace.hrrp_raw.raw_admissions
UNION ALL
SELECT
    'Rows with overall_rating = "Not Available"',
    CAST(SUM(CASE WHEN `Hospital overall rating` = 'Not Available' THEN 1 ELSE 0 END) AS STRING)
FROM workspace.hrrp_raw.raw_admissions
UNION ALL
SELECT
    'Rows with overall_rating = numeric',
    CAST(SUM(CASE WHEN `Hospital overall rating` RLIKE '^[0-9]+$' THEN 1 ELSE 0 END) AS STRING)
FROM workspace.hrrp_raw.raw_admissions
UNION ALL
SELECT
    'Distinct hospital_ownership values',
    array_join(collect_list(DISTINCT `Hospital Ownership`), ', ')
FROM workspace.hrrp_raw.raw_admissions