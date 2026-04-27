-- =======================================================================
-- stg_hrrp_metrics
-- =======================================================================
-- Staging model for CMS Hospital Readmissions Reduction Program (HRRP).
-- Reads from raw_hospital_readmissions, applies the three jobs of staging:
--   1. Rename columns from CMS Title Case + spaces to snake_case
--   2. Cast types from STRING to proper numeric/date types
--   3. Translate CMS missing-value sentinels ('Not Available') to NULL
-- =======================================================================

WITH source AS (
    SELECT * FROM {{ source('hrrp_raw', 'raw_hospital_readmissions') }}
),

cleaned AS (
    SELECT
        -- ---- Identifiers (rename only, no cast needed) ----
        `Facility ID`     AS facility_id,
        `Facility Name`   AS facility_name,
        `State`           AS state_code,

        -- ---- Measure dimension ----
        `Measure Name`    AS measure_name,

        -- ---- Numeric metrics with NULL handling ----
        -- Pattern: CASE-WHEN converts 'Not Available' / 'Too Few to Report' to NULL,
        -- then we CAST the cleaned string to the right numeric type.
        CASE
            WHEN `Number of Discharges` IN ('Not Available', 'Too Few to Report', 'N/A', '')
                THEN NULL
            ELSE CAST(`Number of Discharges` AS INT)
        END AS number_of_discharges,

        CASE
            WHEN `Excess Readmission Ratio` IN ('Not Available', 'Too Few to Report', 'N/A', '')
                THEN NULL
            ELSE CAST(`Excess Readmission Ratio` AS DOUBLE)
        END AS excess_readmission_ratio,

        CASE
            WHEN `Predicted Readmission Rate` IN ('Not Available', 'Too Few to Report', 'N/A', '')
                THEN NULL
            ELSE CAST(`Predicted Readmission Rate` AS DOUBLE)
        END AS predicted_readmission_rate,

        CASE
            WHEN `Expected Readmission Rate` IN ('Not Available', 'Too Few to Report', 'N/A', '')
                THEN NULL
            ELSE CAST(`Expected Readmission Rate` AS DOUBLE)
        END AS expected_readmission_rate,

        CASE
            WHEN `Number of Readmissions` IN ('Not Available', 'Too Few to Report', 'N/A', '')
                THEN NULL
            ELSE CAST(`Number of Readmissions` AS INT)
        END AS number_of_readmissions,

        -- ---- Date columns ----
        -- Start Date came in as STRING; End Date came in as DATE.
        -- We cast Start Date to DATE for symmetry. End Date doesn't need casting
        -- but we wrap it anyway for explicit-type discipline.
        CASE
            WHEN `Start Date` IN ('Not Available', 'N/A', '') THEN NULL
            ELSE CAST(`Start Date` AS DATE)
        END AS measurement_period_start,

        CAST(`End Date` AS DATE) AS measurement_period_end,

        -- ---- Footnote (kept for data lineage; can be analyzed later) ----
        Footnote AS footnote_code

    FROM source
)

SELECT * FROM cleaned