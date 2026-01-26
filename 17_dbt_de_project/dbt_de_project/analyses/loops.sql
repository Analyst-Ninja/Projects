{% set cols = ["booking_id", "nights_booked", "booking_amount"] %}

SELECT 
{% for col in cols %}
    {{col}}
    {% if not loop.last %}, {% endif%}
{% endfor%}
FROM {{ ref('bronze_bookings') }}