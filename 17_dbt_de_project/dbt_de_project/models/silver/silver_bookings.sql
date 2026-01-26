{{
  config(
    materialized = 'incremental',
    unique_key = 'booking_id',
    )
}}

SELECT 
    BOOKING_ID, 
    LISTING_ID,
    BOOKING_DATE, 
    {{ multiply("BOOKING_AMOUNT", "NIGHTS_BOOKED", 2) }} + CLEANING_FEE + SERVICE_FEE AS TOTAL_AMOUNT,
    BOOKING_STATUS,
    CREATED_AT
FROM {{ ref('bronze_bookings') }}