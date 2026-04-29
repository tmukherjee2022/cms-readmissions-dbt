-- =======================================================================
-- int_readmissions_flagged
-- =======================================================================
-- Intermediate model: applies the is_readmission conformance rule to
-- patient admission events.
--
-- Grain: one row per admission event (same as raw_readmissions_events seed).
--
-- Pattern: for each admission, look up the patient's PRIOR discharge using
-- LAG() partitioned by patient_id ordered by admission_date. Then call the
-- is_readmission macro on the pair to determine if this admission counts as
-- a readmission under HRRP rules.
--
-- Critical design choice: the conformance rule lives in the is_readmission
-- macro (macros/is_readmission.sql), NOT here. This model just applies it.
-- If the rule changes, change one line in the macro; this model picks it up.
-- =======================================================================

WITH source AS (
    SELECT * FROM {{ ref('raw_readmissions_events') }}
),

with_prior_admission AS (
    -- Use LAG() to attach each admission's prior discharge date and metadata
    -- to the row, partitioned by patient. The first admission per patient
    -- gets NULL for prior values (no prior admission existed).
    SELECT
        admission_id,
        patient_id,
        facility_id,
        admission_date,
        discharge_date,
        admission_type,
        diagnosis_category,
        length_of_stay,

        -- LAG fetches the value from the prior row in the partition
        LAG(discharge_date) OVER (
            PARTITION BY patient_id
            ORDER BY admission_date ASC
        ) AS prior_discharge_date,

        LAG(admission_id) OVER (
            PARTITION BY patient_id
            ORDER BY admission_date ASC
        ) AS prior_admission_id

    FROM source
),

flagged AS (
    -- Apply the conformance rule via macro. The macro returns a boolean
    -- expression that the warehouse evaluates against each row.
    SELECT
        admission_id,
        patient_id,
        facility_id,
        admission_date,
        discharge_date,
        admission_type,
        diagnosis_category,
        length_of_stay,
        prior_admission_id,
        prior_discharge_date,

        -- Days since prior discharge (NULL if no prior admission)
        DATEDIFF(admission_date, prior_discharge_date) AS days_since_prior_discharge,

        -- THE conformance rule applied here
        {{ is_readmission(
            'prior_discharge_date',
            'admission_date',
            'admission_type',
            'diagnosis_category'
        ) }} AS is_readmission

    FROM with_prior_admission
)

SELECT * FROM flagged