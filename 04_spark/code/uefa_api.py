import requests
import json
import pandas as pd

import uefa_url


# window.environment = 'prd';
#     window.uefaBaseUrl = '//www.uefa.com';
#     window.vsmBaseUrl = '';
#     window.uefaApiBaseUrl = '/api/v1/';
#     window.liveBlogBasePath = '/api/v2/blogs/';

#     window.competitionId = '3';
#     window.competitionFolder = 'uefaeuro';
#     window.competitionBanner = 'uefaeuro';
#     window.competitionTracking = 'euro';
#     window.competitionCode = 'euro2024';
#     window.competitionName = 'uefaeuro';
#     window.competitionUrl = 'uefaeuro';
#     window.isClub = false;
#     window.currentSeason = 2024;
#     window.imgBaseUrl = 'https://img.uefa.com';


# window.matchApiUrl = 'https://match.uefa.com/';
# window.compApiUrl = 'https://comp.uefa.com/';
# window.compStatsApiUrl = 'https://compstats.uefa.com/';
# indow.drawApiUrl = 'https://fsp-draw-service.uefa.com/';
# window.matchStatsApiUrl = 'https://matchstats.uefa.com/';
# window.masApiUrl = 'https://mas.uefa.com/';
# window.domesticApiUrl = 'https://domestic.uefa.com/';
# window.cardApiUrl = 'https://fsp-data-cards-service.uefa.com/';
# window.performanceApiBaseUrl = 'https://fsp-players-ranking-service.uefa.com/';
# window.cobaltApiUrl = 'https://editorial.uefa.com/api/';
# window.cobaltApiKey = 'bc1ff15c-814f-4318-b374-50ad9c1b7294';
# window.cobaltBaseUrl = 'https://editorial.uefa.com/';
# window.cobaltImgBaseUrl = 'https://editorial.uefa.com/';
# window.sponsorApiUrl = 'https://fsp-sponsor-service.uefa.com/';


class UefaApi():
    def __init__(self):
        pass

    @staticmethod
    def get_matches(competition_id: int, season_year: int):
        resp = requests.get(uefa_url.url_matches.format(competition_id=competition_id, season_year=season_year))
        return resp.json()

    @staticmethod
    def get_competitions():
        resp = requests.get(uefa_url.url_competitions)
        return resp.json()

   
    @staticmethod
    def get_stadiums(offset: int):
        resp = requests.get(uefa_url.url_stadiums.format(offset=offset))
        return resp.json()




