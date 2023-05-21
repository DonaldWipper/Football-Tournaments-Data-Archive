import mysql.connector
import json
import os
from collections import defaultdict

# Make a database connection
cnx = mysql.connector.connect(
    user='u2053139_user',
    password='WYRMVegdkW5NbMS',
    host='server159.hosting.reg.ru',
    database="u2053139_euro-stat"
)
cursor = cnx.cursor()

# Query the data
query = (
    "SELECT matches.id, c.caption, c.year, date, local_date, t_home.name AS home_team, t_away.name AS away_team, "
    "places.city, places.stadium, stages.title AS stage "
    "FROM matches "
    "JOIN competitions c ON matches.competition_id = c.id "
    "JOIN stages ON matches.stage_id = stages.id "
    "JOIN teams t_home ON matches.home_team_id = t_home.id "
    "JOIN teams t_away ON matches.away_team_id = t_away.id "
    "JOIN places places ON matches.place_id = places.id "
    "WHERE status_id = 2 AND c.year = 2022"
)

cursor.execute(query)

# Parse the data and generate the slices with dependencies


cur_index = 0
indexes_set = set()
teams = defaultdict(dict)
stages = defaultdict(dict)
dates = defaultdict(dict)
places = defaultdict(dict)
slices = {}
matches_to_index = defaultdict(list)


def generate_next_index():
    if len(indexes_set) == 0:
        indexes_set.add(0)
        return 0
    else:
        new_index = max(indexes_set) + 1
        indexes_set.add(new_index)
        return new_index


def create_team_slice(team_name: str, teams: dict, slices: dict) -> int:
    if team_name not in teams:
        index_ = generate_next_index()
        print(index_)
        slice = {
            "type": "team",
            "value": {"name": team_name},
            "index": index_,
            "dependencies": set(),
            "matches": [],
            "imagePath": f"assets/images/{team_name}.png",
        }
        teams[team_name] = slice
        slices[index_] = slice
        return index_
    else:
        return teams[team_name]["index"]


def create_place_slice(stadium: str, city: str, places: dict, slices: dict) -> int:
    if stadium not in places:
        index_ = generate_next_index()
        slice = {
            "type": "place",
            "value": {"city": city, "stadium": stadium},
            "index": index_,
            "dependencies": set(),
            "matches": [],
        }
        places[stadium] = slice
        slices[index_] = slice
        return index_
    else:
        return places[stadium]["index"]


def create_place_slice(stadium: str, city: str, places: dict, slices: dict) -> int:
    if stadium not in places:
        index_ = generate_next_index()
        slice = {
            "type": "place",
            "value": {"city": city, "stadium": stadium},
            "index": index_,
            "dependencies": set(),
            "matches": [],
        }
        places[stadium] = slice
        slices[index_] = slice
        return index_
    else:
        return places[stadium]["index"]


def create_date_slice(date_str: str, dates: dict, slices: dict) -> int:
    if date_str not in dates:
        index_ = generate_next_index()

        slice = {
            "type": "date",
            "value": {"date": date_str},
            "index": index_,
            "dependencies": set(),
            "matches": [],
        }
        dates[date_str] = slice
        slices[index_] = slice
        return index_
    else:
        return dates[date_str]["index"]


def create_stage_slice(stage: str, stages: dict, slices: dict) -> int:
    if stage not in stages:
        index_ = generate_next_index()
        slice = {
            "type": "stage",
            "value": {"name": stage},
            "index": index_,
            "dependencies": set(),
            "matches": [],
        }
        stages[stage] = slice
        slices[index_] = slice
        return index_
    else:
        return stages[stage]["index"]


def update_slice_dependencies(
        match_id: int, index: int, slices: dict, matches_to_index: dict
):
    indexes = matches_to_index[match_id]
    for i in indexes:
        if i != index and slices[i]["type"] != slices[index]["type"]:
            slices[index]["dependencies"].add(i)


for (
        match_id,
        competition,
        year,
        date,
        local_date,
        home_team,
        away_team,
        city,
        stadium,
        stage,
) in cursor:
    # Create the slice for home team
    create_team_slice(home_team, teams, slices)
    teams[home_team]["matches"].append(match_id)
    matches_to_index[match_id].append(teams[home_team]["index"])

    # Create the slice for away team
    create_team_slice(away_team, teams, slices)
    teams[away_team]["matches"].append(match_id)
    matches_to_index[match_id].append(teams[away_team]["index"])

    create_place_slice(stadium, city, places, slices)
    places[stadium]["matches"].append(match_id)
    matches_to_index[match_id].append(places[stadium]["index"])

    create_stage_slice(stage, stages, slices)
    stages[stage]["matches"].append(match_id)
    matches_to_index[match_id].append(stages[stage]["index"])

    from datetime import datetime

    date_time_obj = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
    date_str = date_time_obj.strftime("%Y-%m-%d")
    time_str = date_time_obj.strftime("%H:%M:%S")

    create_date_slice(date_str, dates, slices)
    dates[date_str]["matches"].append(match_id)
    matches_to_index[match_id].append(dates[date_str]["index"])

    for index in matches_to_index[match_id]:
        update_slice_dependencies(match_id, index, slices, matches_to_index)

print([t["value"]["name"] for t in teams.values()])
print([t["value"]["name"] for t in slices.values() if t["type"] == "team"])

for slice in list(slices.values()):
    slice["dependencies"] = list(slice["dependencies"])

json_data = json.dumps(list(slices.values()), indent=2)
#
print(json_data)
