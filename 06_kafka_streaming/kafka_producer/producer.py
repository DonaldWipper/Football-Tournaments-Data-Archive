from sports_api import Sportsapi
import pandas as pd
import datetime

# грузим только те сезоны, где есть несыгранные матчи
tournaments = Sportsapi.get_all_tournaments()

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
    calendar = Sportsapi.get_tour_calendar(tournament_id=s['tournament_id'], season_id=s['id'])
    import json

    # with open('data.json', 'w') as f:
    #     json.dump(z, f)
    # print(z)
    for item in calendar:
        print(item.keys(), item['stage_name'], len(item["matches"]))
        # stages = [item["stage_name"] for item in calendar]
        # matches = item["matches"]
        # print(stages)
        # import datetime
        # # datetime.datetime.fromtimestamp(v['time']).date()
        # dates = [datetime.datetime.fromtimestamp(v['time']).date() for v in matches]
        # print(len(set(dates)))