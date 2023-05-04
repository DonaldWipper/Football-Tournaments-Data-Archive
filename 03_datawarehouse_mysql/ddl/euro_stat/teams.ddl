create table euro_stat.teams
(
    id          bigint null,
    short_name  text   null,  #  ISO 3166-1 alpha-3
    short_name2 text   null,  #  ISO 3166-1 alpha-2
    name        text   null,
    constraint id
        unique (id)
);
