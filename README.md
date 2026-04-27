# CMS Hospital Readmissions — dbt + Databricks

A working dbt analytics engineering project built on CMS Hospital Readmissions
Reduction Program (HRRP) data, demonstrating source-to-marts transformation
patterns on Databricks. Built as a portable simulation of the architecture
patterns appropriate for hospital-system Quality-domain analytics.

---

## Objective

Demonstrate end-to-end analytics engineering on hospital readmissions data:

- **Three CMS public sources** ingested into Databricks
- **dbt staging layer** that absorbs CMS data quirks (sentinel values, mixed
  types, longitudinal records) and emits clean, typed, snake_case data
- **Marts layer** answering the executive question: which hospitals have
  abnormal readmission rates, what conditions drive them, and what
  financial penalties do they face under HRRP?
- **Conformance rule** for the 30-day readmission definition, encoded as
  a macro and enforced via tests
- **Lineage graph** rendered through `dbt docs` so reviewers can trace
  any final metric back to its source

The project is intentionally aligned to a Quality-domain pilot pattern:
operational quality outcomes (readmission rates from HRRP) joined to
financial consequences (HRR payment adjustments from IPSF) at the
provider level.

---

## Data Schema

### Sources (`workspace.hrrp_raw`)

| Table | Source File | Grain | Rows |
|---|---|---|---|
| `raw_hospital_readmissions` | CMS HRRP FY 2026 | provider × measure | ~18K |
| `raw_providers` | CMS IPPS Impact File (IPSF) | provider × fiscal year | ~1.18M |
| `raw_admissions` | CMS Hospital General Information | one per hospital | ~5.4K |

### Seeds (`workspace.hrrp_seeds`)

| Table | Purpose |
|---|---|
| `cms_provider_type_codes` | CMS two-character provider type taxonomy with HRRP eligibility flags. Encodes a business rule as data so it's auditable and changeable without touching SQL. |

### Staging (`workspace.hrrp_staging`)

Materialized as views. One model per source. Job: clean column names, cast
types, handle CMS sentinel values, assert grain.

| Model | Grain | Notes |
|---|---|---|
| `stg_hrrp_metrics` | provider × measure | Renames Title-Case to snake_case; casts numeric strings to INT/DOUBLE handling 'Not Available' sentinels |
| `stg_admissions` | one row per hospital | Yes/No → BOOLEAN; quality scores cast via `safe_cast_int_string` macro |
| `stg_providers` | one HRRP-eligible hospital, current FY | Filters via JOIN to seed (`is_hrrp_eligible = true`); deduplicates with `ROW_NUMBER() OVER (PARTITION BY providerCcn ORDER BY fiscalYearBeginDate DESC)`; converts BIGINT YYYYMMDD dates to DATE |

### Marts (`workspace.hrrp_marts`) — *in progress*

| Model | Grain | Layer |
|---|---|---|
| `int_providers_enriched` | one row per hospital | Intermediate; joins three staging tables |
| `int_readmissions_flagged` | one row per admission event | Intermediate; applies `is_readmission()` macro |
| `dim_providers` | one row per hospital | Mart; demographic + financial spine |
| `dim_conditions` | one row per HRRP measure type | Mart; six rows for now |
| `fct_readmissions` | one row per readmission event | Mart; references both dims |

---

## Dataflow
Sources (raw_)              Staging (stg_, views)         Intermediate (int_, tables)        Marts (dim_/fct_, tables)
═══════════════              ════════════════════           ═══════════════════════              ═══════════════════════
raw_hospital_readmissions ─► stg_hrrp_metrics ─────────────►                                ┌─► dim_providers
raw_providers ─────────────► stg_providers ─────────────────► int_providers_enriched ──────┤
raw_admissions ────────────► stg_admissions ────────────────►                                └─► dim_conditions
raw_readmissions (seed) ───► stg_readmissions ──────────────► int_readmissions_flagged ────► fct_readmissions
(applies is_readmission macro)
┌─────────────────────┐
                                                                                        │  Executive Dashboard │
                                                                                        │  (Databricks SQL)    │
                                                                                        └─────────────────────┘
The conformance rule (30-day readmission window, excluding planned cancer/
transplant/rehab admissions) lives in a macro applied at the `int_*` layer.
Marts are thin projections that consume already-flagged data — separation
of "rule enforcement" from "presentation" is intentional.

---

## Local Setup

Requires Python 3.11+ and `uv`.

```bash
# Clone and enter the project
git clone git@github.com:tmukherjee2022/cms-readmissions-dbt.git
cd cms-readmissions-dbt

# Create venv and install dbt
uv venv --python 3.11
source .venv/bin/activate
uv pip install dbt-databricks

# Configure profiles.yml at ~/.dbt/profiles.yml with your Databricks
# workspace host, HTTP path, and PAT. Catalog should be `workspace`,
# schema can be your dev identifier (e.g. `dbt_yourname`).

# Verify connection
dbt debug

# Load seed and run staging models
dbt seed
dbt run --select staging
```

---

## Project Structure
cms_databricks/
├── dbt_project.yml             # Project config; routes models to schemas
├── profiles.yml                # NOT committed; lives at ~/.dbt/
├── data_raw/                   # Local copies of source CSVs (gitignored)
├── macros/
│   ├── generate_schema_name.sql  # Override default schema prefixing
│   └── safe_cast.sql             # Reusable type-cast helpers for CMS sentinels
├── models/
│   ├── staging/
│   │   ├── sources.yml           # Source declarations
│   │   ├── stg_hrrp_metrics.sql
│   │   ├── stg_admissions.sql
│   │   └── stg_providers.sql
│   ├── intermediate/             # In progress
│   └── marts/                    # In progress
└── seeds/
└── cms_provider_type_codes.csv  # CMS provider type taxonomy

---

## Caveat — Demo on Free Tier

This project runs on Databricks Community Edition. Some choices reflect
free-tier constraints rather than what production architecture would look
like in a paid environment:

- **Catalog:** Single `workspace` catalog. Production would use Unity Catalog
  with separate `raw`, `prepared`, `curated` catalogs and ACLs scoped to
  curated for downstream consumers.
- **Orchestration:** dbt runs locally rather than via Databricks Workflows.
  Production would orchestrate `dbt build` as a multi-task workflow with
  retry logic, failure routing, and lineage propagation to DataHub.
- **Ingestion:** CSVs uploaded via UI. Production would use Databricks Auto
  Loader for incremental ingestion or Delta Live Tables for declarative
  pipelines with built-in expectations.
- **Patient events:** HIPAA-protected admission data is synthesized at the
  seed layer rather than sourced from a real EHR. Production would integrate
  Epic/Clarity event streams.
- **Compute:** 2X-Small Serverless warehouse with aggressive auto-stop.
  Production would right-size compute to actual workload concurrency.

The architectural patterns demonstrated here — staging-as-translation-layer,
seed-as-business-rule, conformance rules as macros, schema-as-medallion —
are the same patterns that scale to production. Free-tier vs. paid is an
implementation choice; the architecture is the asset.