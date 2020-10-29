import requests
import logging

import pandas as pd
import numpy as np

from multiprocessing import Process


class GameData(Process):

    def __init__(self, starting_point: int, count: int, logger: logging.Logger):
        super().__init__()
        self.starting_point = starting_point
        self.count = count
        self.logger = logger
        self.game_df = pd.DataFrame()

    @staticmethod
    def team_data(df: pd.DataFrame):
        """
        Getting the teams and the scores from the team DataFrame that was passed in.
        :param df: DataFrame containing the scores and names of the teams
        """
        teams = df[df.columns[0]].tolist()
        points = df[df.columns[-1]].tolist()
        return teams, points

    @staticmethod
    def format_game_data_row(df: pd.DataFrame):
        """
        Transforming the data frame passed in the function, to a new dataframe containing one row 
        to append to the overall data frame. This will move the data from the rows
        into columns to represent the different features. 
        """
        def create_name(row_name, team):
            """ Creating a name from the row name and team name. """
            return '{}_{}'.format(team, row_name)

        # creating columns of the home and away team
        transformed_df = pd.DataFrame()
        _, away_team, home_team = df.columns.tolist()
        transformed_df['away_team'] = [away_team]
        transformed_df['home_team'] = [home_team]

        # adding columns to the new dataframe
        for row in df.index:
            rowname, away_value, home_value = df.iloc[row].tolist()
            transformed_df[create_name(rowname.lower(), 'away')] = [away_value]
            transformed_df[create_name(rowname.lower(), 'home')] = [home_value]

        return transformed_df

    @staticmethod
    def make_url(page_number: int):
        """
        Making the url to lookup the data.
        """
        return 'https://www.espn.com/nfl/matchup?gameId={}'.format(page_number)

    def get_data(self, url: str):
        """
        Getting the data from the website. The website will be collected as html and then passed into 
        the pandas read_html to get the data tables from the html file.
        :param url: Website to collect the data from
        """
        try:
            html = requests.get(url).content
            nfl_df_list = pd.read_html(html)
        except Exception as e:
            return pd.DataFrame

        # get the team data from the first pd.DataFrame
        team_df = nfl_df_list[0]
        teams, points = self.team_data(team_df)

        # getting the matchup data
        matchup_columns = np.append('game_stat', teams)
        matchup_df = nfl_df_list[1]
        matchup_df.columns = matchup_columns

        # setting up points information and sending back the new DataFrame
        point_info = np.array([np.append('points', points)])
        matchup_df = matchup_df.append(pd.DataFrame(point_info, columns=matchup_columns))
        matchup_df.index = list(range(len(matchup_df.index)))
        
        return self.format_game_data_row(matchup_df)
    
    def get_game_df(self):
        """
        Function to return the dataframe that has been created from the many url calls.
        :returns: game dataframe
        """
        return self.game_df

    def run(self):
        """
        Overriding the run method from the parent Thread. This method will
        begin the process of gathering data from an nfl url and then will
        return the full DataFrame of data when it reaches it's end. 
        """
        started = False
        for i in range(self.starting_point, self.starting_point+self.count):
            url = self.make_url(i)
            new_data = self.get_data(url)
            
            if not new_data.empty and started:
                self.game_df = pd.concat([self.game_data, new_data])
                self.logger.info(self.game_df.tail())
            
            if not started and not new_data.empty:
                self.game_df = new_data
                started = True
                
            if i % 10 == 0:
                self.logger.info('************* {} *************'.format(i))

