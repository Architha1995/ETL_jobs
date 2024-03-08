
-- Use the `ref` function to select from other models
{{ config(materialized='view') }}

SELECT
    c.id as character_id,
    c.name as character_name,
    c.status,
    c.species
    -- l.name as location_name,
    -- l.type as location_type
FROM public.rick_and_morty_characters c
-- LEFT JOIN public.rick_and_morty_locations l ON c.location_url = l.url