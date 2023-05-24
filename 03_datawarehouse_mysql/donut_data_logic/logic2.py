import mysql.connector
import json
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
query = """
        SELECT matches.id,
               c.caption,
               ms.status,
               c.year,
               date,
               local_date,
               t_home.name  AS home_team,
               t_away.name  AS away_team,
               places.city,
               places.stadium,
               stages.title AS stage,
               matches.total_home_goals,
               matches.total_away_goals,
               matches.penalty_shootout_home_goals,
               matches.penalty_shootout_away_goals,
               goals.text,
               lower(concat(t_home.short_name2, '.svg')) as image_path_home,
               lower(concat(t_away.short_name2, '.svg')) as image_path_away
        FROM matches
                 JOIN competitions c ON matches.competition_id = c.id
                 JOIN stages ON matches.stage_id = stages.id
                 JOIN teams t_home ON matches.home_team_id = t_home.id
                 JOIN teams t_away ON matches.away_team_id = t_away.id
                 JOIN match_status ms on matches.status_id = ms.id
                 JOIN places places ON matches.place_id = places.id
                 JOIN (SELECT match_id, GROUP_CONCAT(text ORDER BY id ASC SEPARATOR ',') AS text
                       FROM goals
                       GROUP BY match_id) AS goals ON matches.id = goals.match_id
        WHERE status_id = 2
          AND c.year = {year}
"""
year = 2022

cursor.execute(query.format(year=year))

# Parse the data and generate the slices with dependencies

cur_index = 0
indexes_set = set()
teams = defaultdict(dict)
stages = defaultdict(dict)
dates = defaultdict(dict)
places = defaultdict(dict)
slices = {}
matches = []
matches_to_index = defaultdict(list)


def generate_next_index():
    if len(indexes_set) == 0:
        indexes_set.add(0)
        return 0
    else:
        new_index = max(indexes_set) + 1
        indexes_set.add(new_index)
        return new_index


def create_team_slice(team_name: str, image_path: str, teams: dict, slices: dict) -> int:
    if team_name not in teams:
        index_ = generate_next_index()
        slice = {
            "type": "team",
            "value": {"name": team_name},
            "index": index_,
            "dependencies": set(),
            "matches": [],
            "imagePath": image_path,
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


def generate_result(total_home_goals: int,
                    total_away_goals: int,
                    penalty_shootout_home_goals: int,
                    penalty_shootout_away_goals: int) -> str:
    if penalty_shootout_home_goals:
        return f"{total_home_goals}({penalty_shootout_home_goals}):{total_away_goals}({penalty_shootout_away_goals})"
    else:
        return f"{total_home_goals}:{total_away_goals}"


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
        status,
        year,
        date,
        local_date,
        home_team,
        away_team,
        city,
        stadium,
        stage,
        total_home_goals,
        total_away_goals,
        penalty_shootout_home_goals,
        penalty_shootout_away_goals,
        text,
        image_path_home,
        image_path_away
) in cursor:
    competition_name = competition
    from datetime import datetime

    date_time_obj = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
    date_str = date_time_obj.strftime("%Y-%m-%d")
    time_str = date_time_obj.strftime("%H:%M:%S")

    matches.append({"match_id": match_id,
                    "status": status,
                    "year": year,
                    "date": date_str,
                    "time": time_str,
                    "home_team": home_team,
                    "away_team": away_team,
                    "city": city,
                    "stadium": stadium,
                    "stage": stage,
                    "total_home_goals": total_home_goals,
                    "total_away_goals": total_away_goals,
                    "penalty_shootout_home_goals": penalty_shootout_home_goals,
                    "penalty_shootout_away_goals": penalty_shootout_away_goals,
                    "text": text,
                    "image_path_home": image_path_home,
                    "image_path_away": image_path_away,
                    "result": generate_result(total_home_goals, total_away_goals, penalty_shootout_home_goals,
                                              penalty_shootout_away_goals, )
                    })

    # Create the slice for home team
    create_team_slice(home_team, image_path_home, teams, slices)
    teams[home_team]["matches"].append(match_id)
    matches_to_index[match_id].append(teams[home_team]["index"])

    # Create the slice for away team
    create_team_slice(away_team, image_path_away, teams, slices)
    teams[away_team]["matches"].append(match_id)
    matches_to_index[match_id].append(teams[away_team]["index"])

    create_place_slice(stadium, city, places, slices)
    places[stadium]["matches"].append(match_id)
    matches_to_index[match_id].append(places[stadium]["index"])

    create_stage_slice(stage, stages, slices)
    stages[stage]["matches"].append(match_id)
    matches_to_index[match_id].append(stages[stage]["index"])

    create_date_slice(date_str, dates, slices)
    dates[date_str]["matches"].append(match_id)
    matches_to_index[match_id].append(dates[date_str]["index"])

    for index in matches_to_index[match_id]:
        update_slice_dependencies(match_id, index, slices, matches_to_index)

# Sort the slices by type
sorted_slices = sorted(slices.values(), key=lambda x: x["type"])
response = {}
for slice in sorted_slices:
    slice["dependencies"] = list(slice["dependencies"])

response['season'] = year
response['competition'] = competition_name
response['slices'] = sorted_slices
response['matches'] = matches
json_data = json.dumps(response, indent=2)
print(json_data)
