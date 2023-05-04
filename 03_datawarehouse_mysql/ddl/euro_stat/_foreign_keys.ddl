ALTER TABLE euro_stat.matches
    ADD FOREIGN KEY (competition_id) REFERENCES euro_stat.competitions (id);

ALTER TABLE euro_stat.matches
    ADD FOREIGN KEY (place_id) REFERENCES euro_stat.places (id);

ALTER TABLE euro_stat.matches
    ADD FOREIGN KEY (away_team_id) REFERENCES euro_stat.teams (id);

ALTER TABLE euro_stat.matches
    ADD FOREIGN KEY (home_team_id) REFERENCES euro_stat.teams (id);

ALTER TABLE euro_stat.matches
    ADD FOREIGN KEY (stage_id) REFERENCES euro_stat.stages (id);

ALTER TABLE euro_stat.matches
    ADD FOREIGN KEY (status_id) REFERENCES euro_stat.match_status (id);

ALTER TABLE euro_stat.players
    ADD FOREIGN KEY (team_id) REFERENCES euro_stat.teams(id);

ALTER TABLE euro_stat.goals
    ADD FOREIGN KEY (match_id) REFERENCES euro_stat.matches(id);

ALTER TABLE euro_stat.goals
    ADD FOREIGN KEY (player_id) REFERENCES euro_stat.players(id);

ALTER TABLE euro_stat.goals
    ADD FOREIGN KEY (team_id) REFERENCES euro_stat.teams(id);




