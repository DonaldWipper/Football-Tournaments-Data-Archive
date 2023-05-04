create table euro_stat.match_status
(
    id     int  not null,
    status text null,
    constraint id
        unique (id)
)
