
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED 5a6b3c35bdb0f7aed382b23216f1cdcc

Script dependencies:

{{ ref('tfm_trip_distance') }}

*/

SELECT * FROM {{ target.schema }}.{{ model.alias }}
