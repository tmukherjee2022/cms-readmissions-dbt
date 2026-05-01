-- =======================================================================
-- generate_schema_name override
-- =======================================================================
-- dbt's default behavior: prepends profile schema to model schema config.
-- Default would produce 'dbt_tanaya_hrrp_staging'. Ugly.
--
-- This override: if a model's +schema config is set, USE IT DIRECTLY.
-- If not set, fall back to profile default. Clean separation between
-- "dbt_tanaya" (sandbox) and the layered schemas (hrrp_staging etc).
--
-- Reference: https://docs.getdbt.com/docs/build/custom-schemas
-- =======================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
