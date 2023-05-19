import requests
import json
import pandas as pd

import url_fifa


class FifaApi():
    def __init__(self):
        pass

    @staticmethod
    def get_matches(id_competition, id_season):
        resp = requests.get(url_fifa.url_matches.format(idCompetition=id_competition, idSeason=id_season))
        # print(resp.json()['Results'])
        df = pd.DataFrame(data=resp.json()['Results'])
        return df

    @staticmethod
    def get_stadiums(count: int, page: int):
        resp = requests.get(url_fifa.url_stadiums)
        # print(resp.json()['Results'])
        df = pd.DataFrame(data=resp.json()['Results'])
        return df

