-- models/staging/game_results.sql
WITH deduplicated_game_results AS (
    SELECT DISTINCT *
    FROM {{ source('fifa_staging', 'game_results') }}
)

SELECT *
FROM deduplicated_game_results;