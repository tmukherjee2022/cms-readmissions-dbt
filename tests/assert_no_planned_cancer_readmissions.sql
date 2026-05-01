-- Singular test enforcing the HRRP conformance rule:
-- a planned admission for cancer treatment must never be flagged as a readmission.
-- This test passes when it returns zero rows.

select
    admission_id,
    diagnosis_category,
    admission_type,
    is_readmission
from {{ ref('fct_readmissions') }}
where is_readmission = true
  and diagnosis_category = 'Cancer Treatment'
  and admission_type = 'Planned'