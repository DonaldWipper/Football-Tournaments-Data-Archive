create table uefa_staging.goals_info
(
    id int,
    player VARCHAR(40),
    countryCode VARCHAR(40),
    minute int,
    second int,
    text VARCHAR(1000)
);


insert into uefa_staging.goals_info
SELECT id,
       player,
       countryCode,
       minute,
       second,
       concat(minute, '` ', player, '(', countryCode, ')') as text
FROM uefa_staging.timeline,
     JSON_TABLE(playerEvents, '$.scorers[*]'
                COLUMNS (
                    player VARCHAR(40) PATH '$.player.translations.name.EN',
                    countryCode VARCHAR(40) PATH '$.player.countryCode',
                    minute int PATH '$.time.minute',
                    second int PATH '$.time.second'
                    )
         ) people;
