create table  sports_ru_staging.tournament_stat_seasons
(
    id int,
    tournament_id int,
    name text,
    dt_updated datetime DEFAULT CURRENT_TIMESTAMP
)
