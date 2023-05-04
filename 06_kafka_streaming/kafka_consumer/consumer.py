from sports_api import Sportsapi
from pymongo import MongoClient
import datetime

client = MongoClient('mongodb://root:example@localhost:27017/')
print(client)

db = client["sports_ru_staging"]
collection = db["tournament_calendar"]

# грузим только те сезоны, где есть несыгранные матчи
tournaments = Sportsapi.all_tournaments()

seasons = []
for tournament_id in list(tournaments['id']):
    x = Sportsapi.get_stat_seasons_by_id(tournament_id)
    if 'name' in x.columns:
        zz = x.sort_values(['name'], ascending=[0])[0:1]
        current_year = datetime.datetime.now().year
        year_seasons = zz.to_dict('records')[0]['name'].split('/')
        if str(current_year) in year_seasons:
            seasons_ = zz.to_dict('records')
            for s in seasons_:
                s['tournament_id'] = tournament_id
            seasons += seasons_
            break

for s in seasons:
    document = Sportsapi.get_tour_calendar(tournament_id=s['tournament_id'], season_id=s['id'])
    print(document)
    result = collection.insert_many(document)
    print(result.ins)
