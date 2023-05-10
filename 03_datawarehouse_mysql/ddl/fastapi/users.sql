DROP TABLE IF EXISTS `users`;
CREATE TABLE `users`
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
) ENGINE = MyISAM
  AUTO_INCREMENT = 7
  DEFAULT CHARSET = utf8mb4;
