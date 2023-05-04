create table euro_stat.goals
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


