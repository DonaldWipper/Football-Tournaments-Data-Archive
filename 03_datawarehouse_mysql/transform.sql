# cписок статусо
insert into match_status
values (1, 'upcoming'),
       (2, 'finished'),
       (3, 'canceled')


# список стадий
INSERT INTO stages(title, id)
VALUES ('Preliminary round', 1);
INSERT INTO stages(title, id)
VALUES ('First round', 2);
INSERT INTO stages(title, id)
VALUES ('Group A', 3);
INSERT INTO stages(title, id)
VALUES ('Group 1', 4);
INSERT INTO stages(title, id)
VALUES ('Group B', 5);
INSERT INTO stages(title, id)
VALUES ('Group 2', 6);
INSERT INTO stages(title, id)
VALUES ('Group C', 7);
INSERT INTO stages(title, id)
VALUES ('Group 3', 8);
INSERT INTO stages(title, id)
VALUES ('Group D', 9);
INSERT INTO stages(title, id)
VALUES ('Group 4', 10);
INSERT INTO stages(title, id)
VALUES ('Group E', 11);
INSERT INTO stages(title, id)
VALUES ('Group 5', 12);
INSERT INTO stages(title, id)
VALUES ('Group F', 13);
INSERT INTO stages(title, id)
VALUES ('Group 6', 14);
INSERT INTO stages(title, id)
VALUES ('Group G', 15);
INSERT INTO stages(title, id)
VALUES ('Group H', 16);
INSERT INTO stages(title, id)
VALUES ('Round of 16', 17);
INSERT INTO stages(title, id)
VALUES ('Quarter-finals', 18);
INSERT INTO stages(title, id)
VALUES ('Semi-finals', 19);
INSERT INTO stages(title, id)
VALUES ('3 AND 4 place', 20);
INSERT INTO stages(title, id)
VALUES ('Final', 21);


# список голов
insert into goals (id, match_id, player_id, text)
select EventId                                                               as id,
       x.match_id,
       IdPlayer,
       replace(replace(replace(CONCAT(MatchMinute, '',
                                      replace(replace(json_extract(
                                                              json_extract(EventDescription, '$[0]'),
                                                              '$.Description'), '"',
                                                      ''), 'scores!!', '')), '\'', '` '),
                       'successfully converts the penalty', 'pen'), '!', '') as text
from timeline_fifa
         join link_match_to_fifa x on match_id_fifa = IdMatch
where lower(TypeLocalized) like '%goal%'
  and lower(TypeLocalized) not like '%attempt%'


# обновить дату в таблице
create table tmp_date_update as
select matches.id,
       um.dateTime,
       um.utcOffsetInHours,
       DATE_FORMAT(
               DATE_ADD(
                       STR_TO_DATE(um.dateTime, '%Y-%m-%dT%H:%i:%sZ'),
                       INTERVAL 2 HOUR
                   ),
               '%Y-%m-%dT%H:%i:%sZ'
           ) as date_local

from matches
         left join link_match_to_uefa lmtu on matches.id = lmtu.match_id
         left join uefa_matches um on lmtu.match_id_uefa = um.id
where date like '%+%'
  and um.dateTime is not null


# cписок стадионов с дупликатами
select *
from (select lat, lng
      from places
      group by lat, lng
      having count(*) > 1) x
         join places p
              on x.lng = p.lng
                  and x.lat = p.lat
order by stadium

# cвязка мест со стадионами
create table link_stadium_to_uefa as
select p.id as place_id,
       s.id as stadium_id_uefa
from uefa_stadiums s
         join (select lng, lat, min(id) as id
               from places
               group by lng, lat) p
              on s.latitude = p.lat
                  and s.longitude = p.lng

insert into link_stadium_to_uefa (place_id, stadium_id_uefa)
values (364, 74459);

insert into places
select 364                                         as id,
       uefa_stadiums.name                          as stadium,
       cityName                                    as city,
       lower(replace(uefa_stadiums.name, ' ', '')) as short_name,
       capacity,
       1476                                        as competition_id,
       latitude,
       longitude
from uefa_stadiums
where id = 74459;


# вставим новый чемпионат
insert into matches (id,
                     competition_id,
                     place_id,
                     stage_id,
                     status_id,
                     date,
                     local_date,
                     home_team_id,
                     away_team_id,
                     goals_home_team,
                     goals_away_team,
                     total_home_goals,
                     total_away_goals,
                     penalty_shootout_home_goals,
                     penalty_shootout_away_goals,
                     game_order)
select uu.id,
       c.id                as competition_id,
       p.id                as place_id,
       s.id                as stage_id,
       match_status.id     as status_id,
       uu.dateTime         as date,
       DATE_FORMAT(
               DATE_ADD(
                       STR_TO_DATE(uu.dateTime, '%Y-%m-%dT%H:%i:%sZ'),
                       INTERVAL utcOffsetInHours HOUR
                   ),
               '%Y-%m-%dT%H:%i:%sZ'
           )               as local_date,
       t_h.id              as home_team_id,
       t_a.id              as away_team_id,
       uu.scoreRegularHome as goals_home_team,
       uu.scoreRegularAway as goals_away_team,
       uu.scoreTotalHome   as total_home_goals,
       uu.scoreTotalAway   as total_away_goals,
       uu.scorePenaltyHome as penalty_shootout_home_goals,
       uu.scorePenaltyAway as penalty_shootout_away_goals,
       uu.matchNumber      as game_order


from (select case
                 when groupName is null then roundName
                 else groupName
                 end as stage_name,
             u.*
      from uefa_matches u
      where u.seasonYear = 2020) uu
         join stages s
              on uu.stage_name = s.title
         join competitions c
              on c.year = uu.seasonYear
         join teams t_h
              on t_h.short_name = uu.homeTeamcountryCode
         join teams t_a
              on t_a.short_name = uu.awayTeamcountryCode

         left join link_stadium_to_uefa lnk_stad
                   on lnk_stad.stadium_id_uefa = uu.stadiumId
         left join places p on lnk_stad.place_id = p.id

         left join match_status on match_status.status = lower(uu.status)

order by uu.id;

