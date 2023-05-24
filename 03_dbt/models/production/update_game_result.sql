-- models/production/update_game_results.sql
WITH updated_game_results AS (SELECT *
                              FROM {{ ref('staging_game_results') } }
WHERE game_date
    > (current_date - interval '30 days')
    )

INSERT INTO {{ target('euro_stat', 'game_results') }}
SELECT *
FROM updated_game_results;
