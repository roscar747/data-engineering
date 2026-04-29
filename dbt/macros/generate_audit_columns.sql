{# Reusable macros for FinSight dbt models. #}

{% macro generate_audit_columns() %}
    current_timestamp                       as _gold_ts,
    '{{ invocation_id }}'                   as _dbt_invocation_id,
    '{{ this.name }}'                       as _dbt_model_name
{% endmacro %}


{% macro freshness_test(model_name, column, hours) %}
    {# Custom freshness test usable as a singular test. #}
    select count(*)
    from {{ ref(model_name) }}
    where {{ column }} < current_timestamp - interval '{{ hours }} hour'
{% endmacro %}
