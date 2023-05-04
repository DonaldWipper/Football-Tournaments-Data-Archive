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

    #
    # def geSeasonsIdByKeyNameAndSeason(self, key_name, season):
    #     tournament = self.get_all_tournament(key_name=key_name).head(1)["id"]
    #     season = self.getStatSeasonsById(tournament, season).any['id
    #     return season
    #
    # def getPlayerInfo(self, tag):
    #     resp = requests.get(url.url_player_info + '?tag=%d' % (tag))
    #     print(url.url_player_info + '?tag=%d' % (tag))
    #     return resp.json()
    #

    #
    # def getPlayerStat(self, tag, season_id=None, tournament_id=None):
    #     if (season_id == None) and (tournament_id == None):
    #         url = url.url_player_stat + '?tag=%d' % tag
    #     else:
    #         url = url.url_player_stat + '?tag=%d&season_id=%d&tournament_id=%d' % (
    #             tag, season_id, tournament_id)
    #     resp = requests.get(url)
    #     # df = pd.DataFrame(data =  resp.json())
    #     return json.loads(resp.text)
    #
    # def getPlayerStatSeason(self, tag, season_id=None, tournament_id=None):
    #     resp = self.getPlayerStat(tag, season_id, tournament_id)
    #     data = resp['all_stat
    #     data['tag_id = tag
    #     return data
    #
    # def getMyTeamInfo(self, team_id):
    #     session = self.session
    #     url = url.url_get % team_id
    #     print(url)
    #     resp = session.get(url.url_get % team_id)
    #     if 'players' in resp.json():
    #         res = resp.json()['players
    #     elif resp.status_code != 200:
    #         res = [{'error': 'cockie was expired'}]
    #     else:
    #         res = [resp.json()]
    #
    #     return res
    #
    # def getAllTeamInfo(self, tournament_id):
    #     session = self.session
    #     url = url.url_tournaments % tournament_id
    #     print('get tournament players from url %s' % url)
    #     resp = session.get(url, headers=self.headers)
    #     res = resp.json()['players
    #     df = pd.DataFrame(data=res)
    #     df.set_index('id')
    #     return df
    #
    # def getMyTeamInfoAllTours(self, team_id):
    #     session = self.session
    #     url = url.url_team_info % team_id
    #     # print(url)
    #     txt = requests.get(url).text
    #     #  "https://www.sports.ru/fantasy/football/team/%d.html",
    #     # parser.tours
    #     # https://www.sports.ru/fantasy/football/team/2104286.html
    #     parser = pc.MyHTMLParser(False)
    #     parser.feed(txt)
    #     team_history = []
    #     # print(parser.tours)
    #     for tour in parser.tours:
    #         url = url.url_points_tour % (team_id, parser.tours[tour])
    #         # print(url)
    #         resp = session.get(url, headers=self.headers)
    #         if 'players' in resp.json():
    #             res = resp.json()['players
    #             for r in res:
    #                 r["tour"] = tour
    #                 # res = [resp.json()]
    #             team_history = team_history + res
    #         elif resp.status_code != 200 and 'team' not in resp.json():
    #             res = [{'error': 'cockie was expired'}]
    #
    #     return pd.DataFrame(data=team_history)
    #
    # def getTeamsTournament(self, season_id, tournament_id, month=None):
    #     params = {"tournament": tournament_id,
    #               "season_id": season_id,
    #               "month": month
    #               }
    #     url = url.url_tournament_calendar
    #     # print(url + '?tornament=%s&season_id=%s '% (str(tournament_id), str(season_id) )
    #     resp = requests.get(url, params=params)
    #     tag_ids = []
    #     for stage in resp.json():
    #         res = stage["matches"]
    #         for match in res:
    #             tag_ids.append(match["command1"]["tag_id"])
    #             tag_ids.append(match["command2"]["tag_id"])
    #     return list(set(tag_ids))
    #
    # def getTeamPlayers(self, tag):
    #     url = url.url_team_stat_players + '?tag=%d' % (tag)
    #     print(url)
    #     resp = requests.get(url)
    #     return json.loads(resp.text)["players"]
    #
    # def getTeamPlayers2(self, tag):
    #     url = url.url_team_players + '?tag=%d' % (tag)
    #     print(url)
    #     resp = requests.get(url)
    #     return json.loads(resp.text)
    #
    # def getPlayersDict(self, season_id, tournament_id, month, list_ids):
    #     players = {}
    #     teams = self.getTeamsTournament(season_id, tournament_id)
    #     print(teams)
    #     pls = []
    #     for team in teams:
    #         try:
    #             pl = self.getTeamPlayers(team)
    #         except:
    #             print("error of loading players")
    #             try:
    #                 pl = self.getTeamPlayers(team)
    #             except:
    #                 print("error of loading players")
    #         try:
    #             pl2 = self.getTeamPlayers2(team)
    #         except:
    #             print("error of loading players")
    #             try:
    #                 pl2 = self.getTeamPlayers2(team)
    #             except:
    #                 print("error of loading players")
    #
    #         for p in pl:
    #             if p['id in list_ids:
    #                 players[p['id] = p['tag_id
    #         for p in pl2:
    #             if p['player_id in list_ids and p['player_id not in players:
    #                 players[p['player_id] = p['tag_id
    #     return players
    #
    # def sendTransfers(self, transfers, team_id):
    #     session = requests.Session()
    #     resp = session.post(url.url_save % team_id, data=transfers, headers=self.headers)
    #
    #     if resp.status_code != 200:
    #         try:
    #             print('team %d send %s' % (team_id, str(resp.json())))
    #             return resp.json()
    #         except:
    #             print('team %d send %s' % (team_id, resp.text))
    #             return resp.text
    #     else:
    #         print('team %d send %s' % (team_id, resp.text))
    #         return {'status': 'OK'}
