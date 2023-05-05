import requests
import pandas as pd
import url


class Sportsapi():
    def __init__(self):
        pass

    @staticmethod
    def all_tournaments(category_id=None, key_name=None):
        if category_id is not None:
            prefix = '?category_id=%d' % category_id
        else:
            prefix = ''
        resp = requests.get(url.url_tournament_all + prefix)
        df = pd.DataFrame(data=resp.json())
        if key_name is not None:
            df = df[df["name"].str.contains(key_name)]
        return df

    @staticmethod
    def get_stat_seasons_by_id(tournament, season=None):
        resp = requests.get(url.url_seasons + '?tournament=%d' % tournament)
        df = pd.DataFrame(data=resp.json())
        if season is not None:
            df = df[df["name"].str.contains(season)]
        return df

    @staticmethod
    def get_tour_info_by_id(tournament_id):
        resp = requests.get(url.url_tournament_info + '?&tournament=%d' % tournament_id)
        return resp.json()

    @staticmethod
    def get_tour_calendar(tournament_id, season_id):
        resp = requests.get(url.url_tournament_calendar + '?&tournament=%d' % tournament_id + '&season=%d' % season_id)
        return resp.json()

    @staticmethod
    def get_playoff_stat(tournament_id, season_id):
        resp = requests.get(url.url_playoff_stat + '?&tournament=%d' % tournament_id + '&season=%d' % season_id)
        return resp.json()

    @staticmethod
    def get_info_by_tag(tag_id):
        resp = requests.get(url.url_tags + '?&tag=%d' % tag_id)
        return resp.json()
