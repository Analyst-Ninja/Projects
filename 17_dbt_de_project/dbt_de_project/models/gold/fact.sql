{% set configs = [
    {
        "table" : "AIRBNB.GOLD.OBT",
        "columns" : "GOLD_OBT.host_id, GOLD_OBT.listing_id, GOLD_OBT.booking_id, GOLD_OBT.total_amount, GOLD_OBT.accommodates, GOLD_OBT.bedrooms, GOLD_OBT.bathrooms, GOLD_OBT.price_per_night, GOLD_OBT.response_rate",
        "alias" : "GOLD_OBT"
    }
] %}

SELECT 
    {{ configs[0]['columns'] }}
FROM
    {{ configs[0]['table'] }} AS {{ configs[0]['alias'] }}