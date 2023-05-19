create table euro_stat.matches
(
    id                          int    null,
    competition_id              bigint null,
    place_id                    bigint null,
    stage_id                    int    null,
    status_id                   int    null,
    date                        text   null, # время игры в зоне UTC '%Y-%m-%dT%H:%i:%s'
    local_date                  text   null, # время игры в локальной зоне
    home_team_id                bigint null,
    away_team_id                bigint null,
    goals_home_team             int    null, # голы в основное время
    goals_away_team             int    null,
    total_home_goals            int    null, # итоговый счет без пенальти
    total_away_goals            int    null,
    penalty_shootout_home_goals int    null, # cерия пенальти забитые голы
    penalty_shootout_away_goals int    null,
    game_order                  int    null,
    constraint id
        unique (id)
);

