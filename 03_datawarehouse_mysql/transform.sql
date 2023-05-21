# cписок статусо
insert into match_status
values (1, 'upcoming'),
       (2, 'finished'),
       (3, 'canceled')


# список стадий
INSERT INTO stages(title, id) VALUES ('Preliminary round', 1);
INSERT INTO stages(title, id) VALUES ('First round', 2);
INSERT INTO stages(title, id) VALUES ('Group A', 3);
INSERT INTO stages(title, id) VALUES ('Group 1', 4);
INSERT INTO stages(title, id) VALUES ('Group B', 5);
INSERT INTO stages(title, id) VALUES ('Group 2', 6);
INSERT INTO stages(title, id) VALUES ('Group C', 7);
INSERT INTO stages(title, id) VALUES ('Group 3', 8);
INSERT INTO stages(title, id) VALUES ('Group D', 9);
INSERT INTO stages(title, id) VALUES ('Group 4', 10);
INSERT INTO stages(title, id) VALUES ('Group E', 11);
INSERT INTO stages(title, id) VALUES ('Group 5', 12);
INSERT INTO stages(title, id) VALUES ('Group F', 13);
INSERT INTO stages(title, id) VALUES ('Group 6', 14);
INSERT INTO stages(title, id) VALUES ('Group G', 15);
INSERT INTO stages(title, id) VALUES ('Group H', 16);
INSERT INTO stages(title, id) VALUES ('Round of 16', 17);
INSERT INTO stages(title, id) VALUES ('Quarter-finals', 18);
INSERT INTO stages(title, id) VALUES ('Semi-finals', 19);
INSERT INTO stages(title, id) VALUES ('3 AND 4 place', 20);
INSERT INTO stages(title, id) VALUES ('Final', 21);


# список голов
insert into goals (id, match_id, player_id, text)
select  EventId as id,
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