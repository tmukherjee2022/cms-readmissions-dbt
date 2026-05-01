-- =======================================================================
-- safe_cast macros
-- =======================================================================
-- Reusable type-cast helpers that handle CMS sentinel values for missing
-- data. Each macro returns NULL for known sentinels and casts otherwise.
--
-- Usage in models:
--   {{ safe_cast_int_string('`Number of Discharges`') }} AS number_of_discharges
-- =======================================================================

{% macro safe_cast_int_string(column) %}
    CASE
        WHEN {{ column }} IN ('Not Available', 'Not Applicable', 'Too Few to Report', 'N/A', '')
            THEN NULL
        ELSE CAST({{ column }} AS INT)
    END
{% endmacro %}


{% macro safe_cast_double_string(column) %}
    CASE
        WHEN {{ column }} IN ('Not Available', 'Not Applicable', 'Too Few to Report', 'N/A', '')
            THEN NULL
        ELSE CAST({{ column }} AS DOUBLE)
    END
{% endmacro %}


{% macro safe_cast_date_string(column) %}
    CASE
        WHEN {{ column }} IN ('Not Available', 'Not Applicable', 'N/A', '')
            THEN NULL
        ELSE CAST({{ column }} AS DATE)
    END
{% endmacro %}
