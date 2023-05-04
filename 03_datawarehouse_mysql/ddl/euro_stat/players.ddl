create table euro_stat.players
(
    id             int  not null,
    number         int  null,
    image          text null,
    first_name     text null,
    last_name      text null,
    position       text null,
    team_id        int  null,
    birthdate      date null,
    constraint id
        unique (id)
);
