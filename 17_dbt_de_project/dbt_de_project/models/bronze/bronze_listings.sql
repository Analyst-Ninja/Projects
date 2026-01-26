{{
  config(
    materialized = 'incremental',
    )
}}
{% set incremental_col = 'created_at'%}


SELECT * FROM {{ source('staging', 'listings') }}

{% if is_incremental() %}
  WHERE {{ incremental_col }} > coalesce((select max({{ incremental_col }}) from {{ this }}), '1900-01-01')
{% endif %}