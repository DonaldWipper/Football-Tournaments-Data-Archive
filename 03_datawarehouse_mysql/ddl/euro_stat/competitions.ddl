create table euro_stat.competitions
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