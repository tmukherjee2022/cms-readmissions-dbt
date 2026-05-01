-- Post-pipeline data quality audit.
-- Run after `dbt build` to verify the pipeline preserved expected quality.
-- Each section is a self-contained query — run individually in Databricks
-- editor, or run all sections together via dbt compile + paste output.

-- Provider join health in int_providers_enriched . Expected gap is
-- ~180 hospitals (HRRP-tracked but not in IPSF). A larger gap signals
-- the CAST join is dropping more matches than expected.

select
    'int_providers_enriched: missing provider data' as audit_check,
    count(*) as total_rows,
    sum(case when missing_provider_data then 1 else 0 end) as rows_missing_provider,
    round(
        100.0 * sum(case when missing_provider_data then 1 else 0 end) / count(*),
        1
    ) as pct_missing_provider
from {{ ref('int_providers_enriched') }};

-- Admissions join health in int_providers_enriched
-- Any meaningful gap signals an HRRP facility that isn't in Hospital
-- General Information — worth flagging individually.

select
    'int_providers_enriched: missing admissions data' as audit_check,
    count(*) as total_rows,
    sum(case when missing_admissions_data then 1 else 0 end) as rows_missing_admissions,
    round(
        100.0 * sum(case when missing_admissions_data then 1 else 0 end) / count(*),
        1
    ) as pct_missing_admissions
from {{ ref('int_providers_enriched') }};

-- CAST join audit 

with provider_ccn_profile as (
    select
        provider_ccn,
        length(provider_ccn) as ccn_length,
        case when provider_ccn != trim(provider_ccn) then true else false end as has_whitespace,
        case when substring(provider_ccn, 1, 1) = '0' then true else false end as starts_with_zero
    from {{ ref('stg_providers') }}
)
select
    'stg_providers: CCN format profile' as audit_check,
    count(*) as total_ccns,
    sum(case when ccn_length != 6 then 1 else 0 end) as ccns_not_6_chars,
    sum(case when has_whitespace then 1 else 0 end) as ccns_with_whitespace,
    sum(case when starts_with_zero then 1 else 0 end) as ccns_with_leading_zero
from provider_ccn_profile;

--Referential integrity audit for fct readmissions
select
    'fct_readmissions: referential integrity' as audit_check,
    count(*) as total_fct_rows,
    sum(case when c.measure_code is null then 1 else 0 end) as orphaned_from_dim_conditions,
    sum(case when p.facility_id is null then 1 else 0 end) as orphaned_from_dim_providers,
    round(
        100.0 * sum(case when c.measure_code is null or p.facility_id is null then 1 else 0 end) / count(*),
        2
    ) as pct_with_any_orphan
from {{ ref('fct_readmissions') }} f
left join {{ ref('dim_conditions') }} c on f.measure_code = c.measure_code
left join {{ ref('dim_providers') }} p on f.facility_id = p.facility_id;