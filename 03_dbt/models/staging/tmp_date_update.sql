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
