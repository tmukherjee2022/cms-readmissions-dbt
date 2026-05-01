-- Staging model for the CMS IPPS Impact File (IPSF).
--
-- Grain: one row per HRRP-eligible hospital, current fiscal year only.
--
-- Source has ~1.18M rows: 70K providers × ~9.8K distinct fiscal year start
-- dates (longitudinal) × all CMS provider types. This staging model:
--   1. Filters to HRRP-eligible providers via JOIN to cms_provider_type_codes seed
--   2. Filters to recent fiscal years, excluding sentinel dates (19000101, 20610701)
--   3. Keeps the most recent record per provider via ROW_NUMBER()
--   4. Casts BIGINT-as-date columns to proper DATE
--   5. Renames camelCase to snake_case


WITH source AS (
    SELECT * FROM {{ source('hrrp_raw', 'raw_providers') }}
),

eligible_provider_types AS (
    SELECT provider_type_code
    FROM {{ ref('cms_provider_type_codes') }}
    WHERE is_hrrp_eligible = true
),

filtered AS (
    SELECT *
    FROM source
    WHERE providerType IN (SELECT provider_type_code FROM eligible_provider_types)
      AND fiscalYearBeginDate BETWEEN 20200101 AND 20251231
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY providerCcn
            ORDER BY fiscalYearBeginDate DESC
        ) AS fy_rank
    FROM filtered
),

most_recent AS (
    SELECT * FROM ranked WHERE fy_rank = 1
),

cleaned AS (
    SELECT
        -- ---- Identifiers ----
        providerCcn                              AS provider_ccn,
        nationalProviderIdentifier               AS national_provider_id,
        providerType                             AS provider_type_code,

        -- ---- Geography ----
        stateCode                                AS state_code,
        countyCode                               AS county_code,
        msaActualGeographicLocation              AS msa_actual_location,
        cbsaActualGeographicLocation             AS cbsa_actual_location,
        censusDivision                           AS census_division,

        -- ---- Hospital characteristics ----
        bedSize                                  AS bed_count,
        caseMixIndex                             AS case_mix_index,
        internsToBedsRatio                       AS interns_to_beds_ratio,
        medicaidRatio                            AS medicaid_ratio,
        supplementalSecurityIncomeRatio          AS ssi_ratio,
        operatingDsh                             AS operating_dsh_payment,
        capitalIndirectMedicalEducationRatio     AS capital_ime_ratio,
        uncompensatedCareAmount                  AS uncompensated_care_amount,

        -- ---- Value-based program participation ----
        vbpParticipantIndicator                  AS vbp_participant_flag,
        vbpAdjustment                            AS vbp_adjustment_factor,
        hrrParticipantIndicator                  AS hrr_participant_flag,
        hrrAdjustment                            AS hrr_adjustment_factor,
        hacReductionParticipantIndicator         AS hac_reduction_participant_flag,
        ehrReductionIndicator                    AS ehr_reduction_flag,

        -- ---- Hospital classification flags ----
        soleCommunityOrMedicareDependentHospitalBaseYear AS sole_community_hospital_indicator,
        specialPaymentIndicator                  AS special_payment_indicator,
        hospitalQualityIndicator                 AS hospital_quality_indicator,
        newHospital                              AS is_new_hospital_flag,
        ltchDppIndicator                         AS ltch_dpp_indicator,

        -- ---- Cost ratios ----
        operatingCostToChargeRatio               AS operating_cost_to_charge_ratio,
        capitalCostToChargeRatio                 AS capital_cost_to_charge_ratio,
        caseMixAdjustedCostPerDischarge_PpsFacilitySpecificRate
                                                 AS case_mix_adjusted_cost_per_discharge,

        -- ---- Pass-through amounts ----
        passThroughAmountForCapital              AS pass_through_capital,
        passThroughAmountForDirectMedicalEducation
                                                 AS pass_through_direct_medical_ed,
        passThroughAmountForDirectGraduateMedicalEducation
                                                 AS pass_through_direct_graduate_med_ed,
        passThroughAmountForOrganAcquisition     AS pass_through_organ_acquisition,
        passThroughAmountForKidneyAcquisition    AS pass_through_kidney_acquisition,
        passThroughAmountForAllogenicStemCellAcquisition
                                                 AS pass_through_stem_cell_acquisition,
        passThroughTotalAmount                   AS pass_through_total,

        -- ---- Dates: BIGINT YYYYMMDD format → DATE ----
        to_date(CAST(fiscalYearBeginDate AS STRING), 'yyyyMMdd') AS fiscal_year_begin_date,
        to_date(CAST(fiscalYearEndDate AS STRING), 'yyyyMMdd')   AS fiscal_year_end_date,
        to_date(CAST(effectiveDate AS STRING), 'yyyyMMdd')       AS effective_date,
        to_date(CAST(exportDate AS STRING), 'yyyyMMdd')          AS export_date,

        -- 20610701 = "not terminated" sentinel; convert to NULL.
        CASE
            WHEN terminationDate = 20610701 THEN NULL
            WHEN terminationDate = 19000101 THEN NULL
            ELSE to_date(CAST(terminationDate AS STRING), 'yyyyMMdd')
        END AS termination_date,

        -- ---- Audit columns ----
        lastUpdated                              AS last_updated_raw,
        intermediaryNumber                       AS medicare_intermediary_id

    FROM most_recent
)

SELECT * FROM cleaned
