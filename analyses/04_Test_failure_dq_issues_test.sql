--In a CI/CD pipeline, dbt build would run these automatically and block
-- promotion if any failed.
--These three tests are the ones I rely on most. The unique-facility-id one caught a real bug — federal hospitals appearing under civilian CCNs. Without that test, I'd have shipped a dashboard with double-counted readmissions for 8 hospitals and never known.

-- Test 1: Are there duplicate facility_ids in stg_admissions? 
SELECT 'unique_facility_id_in_stg_admissions' AS test_name,
       COUNT(*) - COUNT(DISTINCT facility_id) AS violation_count,
       CASE WHEN COUNT(*) = COUNT(DISTINCT facility_id) THEN 'PASS' ELSE 'FAIL' END AS status
FROM workspace.hrrp_staging.stg_admissions

UNION ALL

-- Test 2: Are there any non-HRRP measure types in stg_hrrp_metrics?
SELECT 'accepted_values_measure_name',
       SUM(CASE WHEN measure_name NOT IN (
           'READM-30-AMI-HRRP', 'READM-30-HF-HRRP', 'READM-30-PN-HRRP',
           'READM-30-COPD-HRRP', 'READM-30-HIP-KNEE-HRRP', 'READM-30-CABG-HRRP'
       ) THEN 1 ELSE 0 END),
       CASE WHEN SUM(CASE WHEN measure_name NOT IN (
           'READM-30-AMI-HRRP', 'READM-30-HF-HRRP', 'READM-30-PN-HRRP',
           'READM-30-COPD-HRRP', 'READM-30-HIP-KNEE-HRRP', 'READM-30-CABG-HRRP'
       ) THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM workspace.hrrp_staging.stg_hrrp_metrics

UNION ALL

-- Test 3: Composite uniqueness on (facility_id, measure_name) 
SELECT 'unique_combination_facility_measure',
       COUNT(*) - COUNT(DISTINCT CONCAT(facility_id, '|', measure_name)),
       CASE WHEN COUNT(*) = COUNT(DISTINCT CONCAT(facility_id, '|', measure_name))
            THEN 'PASS' ELSE 'FAIL' END
FROM workspace.hrrp_staging.stg_hrrp_metrics