create table euro_stat.places
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
