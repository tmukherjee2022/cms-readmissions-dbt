-- =======================================================================
-- fct_readmissions
-- =======================================================================
-- Fact table for hospital readmission events.
--
-- Grain: one row per patient admission event. Each row represents an
-- admission that may or may not be flagged as a readmission under HRRP rules.
--
-- Foreign keys:
--   facility_id  -> dim_providers.facility_id
--   measure_code -> dim_conditions.measure_code
--
-- Design note: diagnosis_category from the seed is mapped to HRRP
-- measure_code inline via CASE. In production, this mapping would live in a
-- separate seed (e.g., diagnosis_to_measure_mapping.csv) for auditability.
-- Inline mapping is acceptable for a six-value lookup demo.
-- =======================================================================

WITH source AS (
    SELECT * FROM {{ ref('int_readmissions_flagged') }}
),

mapped AS (
    SELECT
        -- ---- Identifiers ----
        admission_id,
        patient_id,

        -- ---- Foreign keys ----
        facility_id,
        CASE diagnosis_category
            WHEN 'Heart Failure'           THEN 'READM-30-HF-HRRP'
            WHEN 'AMI'                     THEN 'READM-30-AMI-HRRP'
            WHEN 'Pneumonia'               THEN 'READM-30-PN-HRRP'
            WHEN 'COPD'                    THEN 'READM-30-COPD-HRRP'
            WHEN 'Hip Replacement'         THEN 'READM-30-HIP-KNEE-HRRP'
            WHEN 'CABG'                    THEN 'READM-30-CABG-HRRP'
            ELSE NULL  -- Surgical Complication, Cancer Treatment, Rehab don't map to HRRP measures
        END AS measure_code,

        -- ---- Event timestamps ----
        admission_date,
        discharge_date,
        prior_admission_id,
        prior_discharge_date,

        -- ---- Metrics ----
        length_of_stay,
        days_since_prior_discharge,

        -- ---- The conformance-rule flag ----
        is_readmission,

        -- ---- Audit ----
        admission_type,
        diagnosis_category  -- Kept alongside measure_code for dashboard display

    FROM source
)

SELECT * FROM mapped