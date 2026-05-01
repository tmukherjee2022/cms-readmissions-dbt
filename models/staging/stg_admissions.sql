
-- Staging model for CMS Hospital General Information.
-- This is hospital METADATA, not
-- admission events. One row per Medicare-certified hospital.
--
-- Three jobs of staging applied:
--   1. Rename CMS Title Case columns to snake_case
--   2. Cast STRING-as-numeric columns to proper INT/DOUBLE types
--   3. Translate 'Not Available' / 'Not Applicable' sentinels to NULL
WITH source AS (
    SELECT * FROM {{ source('hrrp_raw', 'raw_admissions') }}
),

cleaned AS (
    SELECT
        -- ---- Identifiers ----
        CAST(`Facility ID` AS BIGINT)             AS facility_id,
        `Facility Name`                           AS facility_name,
        `Address`                                 AS address,
        `City/Town`                               AS city,
        `State`                                   AS state_code,
        `ZIP Code`                                AS zip_code,
        `County/Parish`                           AS county,
        `Telephone Number`                        AS phone_number,

        `Hospital Type`                           AS hospital_type,
        `Hospital Ownership`                      AS hospital_ownership,

        CASE `Emergency Services`
            WHEN 'Yes' THEN TRUE
            WHEN 'No'  THEN FALSE
            ELSE NULL
        END AS has_emergency_services,

        CASE `Meets criteria for birthing friendly designation`
            WHEN 'Yes' THEN TRUE
            WHEN 'No'  THEN FALSE
            ELSE NULL
        END AS is_birthing_friendly,

        CASE
            WHEN `Hospital overall rating` IN ('Not Available', 'Not Applicable', '')
                THEN NULL
            ELSE CAST(`Hospital overall rating` AS INT)
        END AS overall_rating,

        `Hospital overall rating footnote`        AS overall_rating_footnote,

        -- (MORT)
        {{ safe_cast_int_string('`Count of Facility MORT Measures`') }} AS mort_measures_count,
        {{ safe_cast_int_string('`Count of MORT Measures Better`') }}    AS mort_better_count,
        {{ safe_cast_int_string('`Count of MORT Measures No Different`') }} AS mort_same_count,
        {{ safe_cast_int_string('`Count of MORT Measures Worse`') }}     AS mort_worse_count,
        `MORT Group Footnote`                                            AS mort_group_footnote,

        -- Safety
        {{ safe_cast_int_string('`Count of Facility Safety Measures`') }}      AS safety_measures_count,
        {{ safe_cast_int_string('`Count of Safety Measures Better`') }}        AS safety_better_count,
        {{ safe_cast_int_string('`Count of Safety Measures No Different`') }}  AS safety_same_count,
        {{ safe_cast_int_string('`Count of Safety Measures Worse`') }}         AS safety_worse_count,
        `Safety Group Footnote`                                                AS safety_group_footnote,

        -- (READM)
        {{ safe_cast_int_string('`Count of Facility READM Measures`') }}      AS readm_measures_count,
        {{ safe_cast_int_string('`Count of READM Measures Better`') }}        AS readm_better_count,
        {{ safe_cast_int_string('`Count of READM Measures No Different`') }}  AS readm_same_count,
        {{ safe_cast_int_string('`Count of READM Measures Worse`') }}         AS readm_worse_count,
        `READM Group Footnote`                                                AS readm_group_footnote,

        -- (Pt Exp)
        {{ safe_cast_int_string('`Count of Facility Pt Exp Measures`') }}     AS ptexp_measures_count,
        `Pt Exp Group Footnote`                                               AS ptexp_group_footnote,

        -- (TE)
        {{ safe_cast_int_string('`Count of Facility TE Measures`') }}         AS te_measures_count,
        `TE Group Footnote`                                                   AS te_group_footnote

    FROM source
    WHERE `Hospital Ownership` NOT IN (
        'Department of Defense',
        'Veterans Health Administration'
    )
)
SELECT * FROM cleaned

-- HRRP scope filter: exclude federal facilities. VA and DoD hospitals
    -- appear in Hospital General Information for completeness but are not
    -- HRRP-eligible (they operate under separate quality reporting frameworks).
   