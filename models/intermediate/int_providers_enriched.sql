-- =======================================================================
-- int_providers_enriched
-- =======================================================================
-- Intermediate model: provider spine for the HRRP analysis.
--
-- Grain: one row per hospital with at least one HRRP measure.
--   Spine = stg_hrrp_metrics (rolled up to facility level)
--   LEFT JOIN stg_providers ON CCN match (financial / operational attrs)
--   LEFT JOIN stg_admissions ON facility_id match (demographic / quality attrs)
--
-- Why HRRP as spine: the project's central question is "how do HRRP outcomes
-- relate to provider characteristics?" Choosing the spine that maximizes HRRP
-- coverage matches the question. Hospitals without IPSF records (~180) will
-- have NULL financial data — surfaced as a data quality finding rather than
-- silently dropped.
--
-- JOIN type discipline: facility_id from HRRP/Hospital General Information
-- is BIGINT; provider_ccn from IPSF is STRING. CASTs in JOINs to align types.
-- =======================================================================

WITH hrrp_metrics AS (
    -- Roll up six measure rows per hospital into one row.
    -- Excludes nulls in averages so hospitals with partial coverage aren't
    -- penalized by missing measures.
    SELECT
        facility_id,
        AVG(excess_readmission_ratio)              AS avg_excess_readmission_ratio,
        SUM(number_of_readmissions)                AS total_readmissions,
        SUM(number_of_discharges)                  AS total_discharges,
        COUNT(DISTINCT CASE
            WHEN excess_readmission_ratio IS NOT NULL THEN measure_name
        END)                                        AS measures_with_data
    FROM {{ ref('stg_hrrp_metrics') }}
    GROUP BY facility_id
),

providers AS (
    SELECT
        provider_ccn,
        state_code        AS provider_state_code,
        bed_count,
        case_mix_index,
        hrr_adjustment_factor,
        vbp_adjustment_factor,
        medicaid_ratio,
        operating_dsh_payment,
        fiscal_year_begin_date
    FROM {{ ref('stg_providers') }}
),

admissions AS (
    SELECT
        facility_id,
        facility_name,
        address,
        city,
        state_code        AS admissions_state_code,
        hospital_type,
        hospital_ownership,
        has_emergency_services,
        overall_rating,
        readm_better_count,
        readm_same_count,
        readm_worse_count
    FROM {{ ref('stg_admissions') }}
),

joined AS (
    SELECT
        -- ---- Identifiers ----
        h.facility_id,
        a.facility_name,
        COALESCE(p.provider_state_code, a.admissions_state_code) AS state_code,

        -- ---- Demographics from admissions ----
        a.address,
        a.city,
        a.hospital_type,
        a.hospital_ownership,
        a.has_emergency_services,
        a.overall_rating,

        -- ---- Operational from providers ----
        p.bed_count,
        p.case_mix_index,
        p.medicaid_ratio,

        -- ---- Financial penalty exposure (the Quality+Finance story) ----
        p.hrr_adjustment_factor,
        p.vbp_adjustment_factor,
        p.operating_dsh_payment,

        -- ---- HRRP outcomes (rolled up) ----
        h.avg_excess_readmission_ratio,
        h.total_readmissions,
        h.total_discharges,
        h.measures_with_data,

        -- ---- Cross-validation: CMS star-rating readmission family ----
        a.readm_better_count,
        a.readm_same_count,
        a.readm_worse_count,

        -- ---- Data quality flags ----
        CASE WHEN p.provider_ccn IS NULL THEN TRUE ELSE FALSE END AS missing_provider_data,
        CASE WHEN a.facility_id IS NULL  THEN TRUE ELSE FALSE END AS missing_admissions_data,

        -- ---- Audit ----
        p.fiscal_year_begin_date

    FROM hrrp_metrics h
    LEFT JOIN providers p
    ON CAST(h.facility_id AS STRING) = p.provider_ccn
    LEFT JOIN admissions a
        ON h.facility_id = a.facility_id
)

SELECT * FROM joined
