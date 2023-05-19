create table matches
(
    id                          int    null,
    competition_id              bigint null,
    place_id                    bigint null,
    stage_id                    int    null,
    status_id                   int    null,
    date                        text   null comment 'время игры в зоне UTC \'%Y-%m-%dT%H:%i:%s\'',
    local_date                  text   null comment 'время игры в локальной зоне', #
    home_team_id                bigint null,
    away_team_id                bigint null,
    goals_home_team             int    null comment 'голы в основное время',
    goals_away_team             int    null comment 'итоговый счет без пенальти',
    total_home_goals            int    null,
    total_away_goals            int    null,
    penalty_shootout_home_goals int    null comment 'cерия пенальти забитые голы', #
    penalty_shootout_away_goals int    null,
    game_order                  int    null,
    constraint id
        unique (id)
);


create table competitions
(
    id                   bigint null,
    number_of_games      bigint not null,
    number_of_match_days bigint not null,
    number_of_teams      bigint not null,
    last_updated         text   not null,
    caption              text   not null,
    year                 int    not null,
    league               text   not null,
    url                  text   null,
    constraint id
        unique (id)
);


create table goals
(
    id        bigint not null,
    match_id  bigint not null,
    player_id bigint not null,
    team_id   bigint not null,
    minute    int,
    second    int,
    text      text   null,
    constraint id
        unique (id)
);


create table places
(
    id             bigint not null,
    stadium        text   null,
    city           text   null,
    short_name     text   null,
    capacity       int    null,
    competition_id bigint null,
    lat            double null,
    lng            double null,
    constraint id
        unique (id)
);




create table euro_stat.match_status
(
    id     int  not null,
    status text null,
    constraint id
        unique (id)
);




