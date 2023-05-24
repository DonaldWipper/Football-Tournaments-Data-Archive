create table link_stadium_to_uefa as
select p.id,
       s.id as stadium_id_uefa
from uefa_stadiums s
         join (select lng, lat, min(id) as id
               from places
               group by lng, lat) p
              on s.latitude = p.lat
                  and s.longitude = p.lng

