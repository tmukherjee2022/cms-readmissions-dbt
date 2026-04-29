-- =======================================================================
-- dim_providers
-- =======================================================================
-- Provider dimension table. One row per HRRP-eligible hospital.
--
-- Grain: one row per facility_id (natural key from CMS CCN system).
--
-- Design: thin SELECT over int_providers_enriched. No new business logic;
-- dimension exists to provide stable, queryable provider attributes for
-- joining to fact tables. Includes data quality flags so dashboards can
-- expose which hospitals have incomplete data.
-- =======================================================================

WITH source AS (
    SELECT * FROM {{ ref('int_providers_enriched') }}
),

renamed AS (
    SELECT
        -- ---- Primary key (natural) ----
        facility_id,

        -- ---- Identifying attributes ----
        facility_name,
        state_code,
        address,
        city,

        -- ---- Hospital classification ----
        hospital_type,
        hospital_ownership,
        has_emergency_services,
        bed_count,

        -- ---- Quality indicators ----
        overall_rating,
        avg_excess_readmission_ratio,
        measures_with_data,

        -- ---- Financial penalty exposure ----
        hrr_adjustment_factor,
        vbp_adjustment_factor,
        case_mix_index,
        medicaid_ratio,

        -- ---- Data quality flags (for dashboard transparency) ----
        missing_provider_data,
        missing_admissions_data

    FROM source
)

SELECT * FROM renamed