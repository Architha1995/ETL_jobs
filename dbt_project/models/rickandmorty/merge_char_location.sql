
-- Use the `ref` function to select from other models
{{ config(materialized='view') }}

SELECT *
FROM public.rick_and_morty_characters c
INNER JOIN public.rick_and_morty_locations l ON C.character_location_name = L.location_name