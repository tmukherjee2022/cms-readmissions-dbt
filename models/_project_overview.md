{% docs __overview__ %}

# CMS Hospital Readmissions — dbt + Databricks

A working dbt analytics engineering project built on CMS Hospital Readmissions
Reduction Program (HRRP) data, demonstrating source-to-marts transformation
patterns on Databricks. Built as a portable simulation of the architecture
patterns appropriate for hospital-system Quality-domain analytics.

## What this project demonstrates

- **Source-to-marts transformation pipeline** with explicit layering (raw → staging → intermediate → marts)
- **Conformance rules encoded as macros** — the HRRP 30-day readmission rule lives in `macros/is_readmission.sql` as a reusable, versionable, testable artifact
- **Tests as conformance enforcement** — 13+ data tests gate the staging layer; relationships tests propagate across the dimensional model
- **Data quality flags surfaced, not silenced** — hospitals missing financial or demographic data are flagged in the marts and exposed in the dashboard
- **Architectural choices documented in code** — descriptions in YAML, decisions in commit messages, design rationale in `/docs`

## Conformance rules in this project

The most important architectural pattern: **conformance rules live in `macros/`**, applied by intermediate models, validated by tests in `_int_models.yml` and `_marts_models.yml`.

The HRRP readmission rule:
> An admission is a readmission IF it occurs within 30 days of a previous discharge from the same patient AND the admission type is not in the planned-exclusion list (Cancer Treatment, Rehabilitation, Transplant).
{% raw %}
This rule lives in **one file**: `macros/is_readmission.sql`. Anywhere the project asks "is this a readmission?" — the macro is called. If CMS changes the rule (e.g., 60-day window, new exclusion category), one line changes and every downstream model picks it up. **Same pattern would scale to UPHS Epic data: encounter-grouping rules, patient-status rules, charge-event classification rules — all live in `macros/`, applied via `{{ rule_name(...) }}` in models, tested via YAML.**
{% endraw %}
## Architecture overview

| Layer | Schema | Materialization | Purpose |
|---|---|---|---|
| Sources | `workspace.hrrp_raw` | external | Raw CMS files loaded via Databricks UI |
| Seeds | `workspace.hrrp_seeds` | tables | Reference data: provider type codes, synthesized readmission events |
| Staging | `workspace.hrrp_staging` | views | One per source. Renames, casts, sentinel handling. |
| Intermediate | `workspace.hrrp_marts` | tables | Joins, rollups, conformance-rule application. |
| Marts | `workspace.hrrp_marts` | tables | Dimensional model: dim_providers, dim_conditions, fct_readmissions. |

## Production caveats

This runs on Databricks Community Edition (free tier). See README for the full list of limitations and what they would look like in a paid Databricks production environment. Briefly: real ingestion would use Auto Loader or Delta Live Tables; orchestration would use Databricks Workflows; patient events would come from Epic; Unity Catalog would provide governance.

The architectural patterns demonstrated here scale to production unchanged.

{% enddocs %}