{% macro is_high_risk(amount_col, fraud_col, threshold=1000) %}
    case
        when {{ fraud_col }} = 1 and {{ amount_col }} >= {{ threshold }}
            then 'high'
        when {{ fraud_col }} = 1 and {{ amount_col }} < {{ threshold }}
            then 'medium'
        when {{ fraud_col }} = 0 and {{ amount_col }} >= {{ threshold }}
            then 'elevated'
        else 'low'
    end
{% endmacro %}