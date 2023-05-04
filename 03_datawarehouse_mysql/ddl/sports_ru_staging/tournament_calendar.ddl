create table  sports_ru_staging.tournament_calendar
(
    id int,
    attendance int,
    time int,
    year int,
    date date,
    tournament_name text,
    tournament_id int,
    stage_name text,
    only_date bool,
    state_id int,
    state text,
    status_id int,
    status_name text,
    command1 json,
    command2 json,
    playoff_position int,
    final_type int,
    other_matches json,
    dt_updated datetime DEFAULT CURRENT_TIMESTAMP
);
