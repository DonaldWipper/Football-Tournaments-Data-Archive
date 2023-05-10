CREATE DATABASE fifa_staging;
CREATE DATABASE uefa_staging;
CREATE DATABASE sports_ru_staging;
CREATE DATABASE euro_stat;
CREATE DATABASE sources_links;
CREATE DATABASE fastapi;
CREATE DATABASE euro_stat_clubs;
CREATE DATABASE clubelo_staging;
CREATE DATABASE elorating_staging;


CREATE TABLE `fastapi.users`
(
    `id`              int(10) unsigned NOT NULL AUTO_INCREMENT,
    `username`        varchar(255)          DEFAULT NULL,
    `email`           varchar(255)          DEFAULT NULL,
    `salt`            varchar(255)          DEFAULT NULL,
    `hashed_password` varchar(255)          DEFAULT NULL,
    `bio`             varchar(255)          DEFAULT NULL,
    `image`           varchar(255)          DEFAULT NULL,
    `created_at`      timestamp        NULL DEFAULT NULL,
    `updated_at`      timestamp        NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE = MyISAM;