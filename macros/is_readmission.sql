-- =======================================================================
-- is_readmission macro
-- =======================================================================
-- The HRRP conformance rule, encoded as a reusable SQL expression.
--
-- An admission is a readmission if and only if:
--   1. It occurs within 30 days of a previous discharge from the same patient
--      (date difference 0-30 days, inclusive on both ends)
--   2. AND the admission type is NOT in the planned-exclusion list
--      (Cancer Treatment, Rehabilitation, etc. are excluded by HRRP rules)
--
-- Parameters:
--   - prior_discharge_date: DATE, the discharge date of the previous admission
--   - current_admission_date: DATE, the admission date being evaluated
--   - current_admission_type: STRING, e.g., 'Emergency', 'Planned'
--   - current_diagnosis_category: STRING, e.g., 'Heart Failure', 'Cancer Treatment'
--
-- Returns: BOOLEAN expression evaluating to TRUE if this is a readmission.
--
-- Usage in a model:
--   SELECT
--       admission_id,
--       {{ is_readmission(
--           'prior_discharge_date',
--           'admission_date',
--           'admission_type',
--           'diagnosis_category'
--       ) }} AS is_readmission_flag
--   FROM ...
-- =======================================================================

{% macro is_readmission(
    prior_discharge_date,
    current_admission_date,
    current_admission_type,
    current_diagnosis_category
) %}

    (
        -- Rule 1: Time window (0-30 days, inclusive)
        DATEDIFF({{ current_admission_date }}, {{ prior_discharge_date }}) BETWEEN 0 AND 30

        -- Rule 2: Exclude planned admissions for cancer, rehab, transplant
        AND NOT (
            {{ current_admission_type }} = 'Planned'
            AND {{ current_diagnosis_category }} IN (
                'Cancer Treatment',
                'Rehabilitation',
                'Transplant'
            )
        )

        -- Rule 3: Must have a prior discharge (NULL = no prior admission, not a readmission)
        AND {{ prior_discharge_date }} IS NOT NULL
    )

{% endmacro %}