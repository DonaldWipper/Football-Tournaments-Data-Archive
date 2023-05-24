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