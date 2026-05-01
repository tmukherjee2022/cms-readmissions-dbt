-- Readmission rate by condition and state.
-- Powers the dashboard tile: "Where do which conditions readmit most?"
-- Joins fct_readmissions to dim_conditions for condition_name,
-- and to dim_providers for state. Filters out rows where the condition
-- lookup didn't match (measure codes outside the HRRP-tracked set).

select
    c.condition_name,
    p.state,
    count(*) as total_admissions,
    sum(case when f.is_readmission then 1 else 0 end) as readmission_count,
    round(
        100.0 * sum(case when f.is_readmission then 1 else 0 end) / count(*),
        1
    ) as readmission_rate_pct
from {{ ref('fct_readmissions') }} f
left join {{ ref('dim_conditions') }} c
    on f.measure_code = c.measure_code
left join {{ ref('dim_providers') }} p
    on f.provider_id = p.provider_id
where c.condition_name is not null
group by c.condition_name, p.state
order by readmission_rate_pct desc