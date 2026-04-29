-- =======================================================================
-- dim_conditions
-- =======================================================================
-- HRRP measure-type dimension. Six rows, one per HRRP condition.
--
-- Grain: one row per measure_code.
--
-- Design: built inline via UNION ALL (rather than as a seed) because the six
-- HRRP measures are stable, well-defined CMS categories. Inline approach
-- keeps the canonical list visible in one place. If CMS adds a 7th measure
-- type in 2027 (rare but possible), one new line gets added here.
--
-- This dimension is the JOIN target for fct_readmissions. The measure_code
-- column matches the measure_name field in stg_hrrp_metrics.
-- =======================================================================

WITH conditions AS (
    SELECT 'READM-30-AMI-HRRP'         AS measure_code,
           'Heart Attack (AMI)'         AS condition_name,
           'Cardiac'                    AS condition_category,
           1                            AS sort_order
    UNION ALL
    SELECT 'READM-30-HF-HRRP', 'Heart Failure', 'Cardiac', 2
    UNION ALL
    SELECT 'READM-30-PN-HRRP', 'Pneumonia', 'Respiratory', 3
    UNION ALL
    SELECT 'READM-30-COPD-HRRP', 'COPD', 'Respiratory', 4
    UNION ALL
    SELECT 'READM-30-HIP-KNEE-HRRP', 'Hip/Knee Replacement', 'Surgical', 5
    UNION ALL
    SELECT 'READM-30-CABG-HRRP', 'Coronary Artery Bypass (CABG)', 'Surgical', 6
)

SELECT * FROM conditions