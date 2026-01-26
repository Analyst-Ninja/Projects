{% macro tag(amount) %}
    CASE
        WHEN {{amount}} < 100 THEN 'Low'
        WHEN {{amount}} < 200 THEN 'Medium'
        ELSE 'High'
    END
{% endmacro %}