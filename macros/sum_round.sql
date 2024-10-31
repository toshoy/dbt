{% macro SUM_ROUND(column_name, scale = 2 ) %}

    ROUND(SUM({{column_name}}),{{scale}})    

{% endmacro %}


{% macro COALESCE_ROUND(column_name, scale = 2 ) %}

    COALESCE(ROUND({{column_name}},2),0)  

{% endmacro %}