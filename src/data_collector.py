import pandas as pd
import numpy as np
import requests
import os
import time
import random

from pathlib import Path
from log_cfb import CFBDataLogger

from multiprocessing import Pool

# logger for the program
cfb_log = CFBDataLogger()


def team_data(df: pd.DataFrame):
    """
    Getting the teams and the scores from the team DataFrame that was passed in.
    :param df: DataFrame containing the scores and names of the teams
    """
    teams = df[df.columns[0]].tolist()
    points = df[df.columns[-1]].tolist()
    return teams, points


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
        row_name, away_value, home_value = df.iloc[row].tolist()
        transformed_df[create_name(row_name.lower(), 'away')] = [away_value]
        transformed_df[create_name(row_name.lower(), 'home')] = [home_value]

    return transformed_df


def make_url(page_number: str):
    """
    Making the url to lookup the data.
    """
    return 'https://www.espn.com/nfl/matchup?gameId={}'.format(page_number)


def handle_error(error: str):
    """
    Generic function to handle errors.
    :param error: error message from the error
    """
    cfb_log.info(error)
    return pd.DataFrame()


def get_df(nfl_df_list: list, df_shape: tuple):
    """
    Function to return the correct df of the team data. This ensures the correct data is given to the team_df
    :param nfl_df_list: list of data frames
    :param df_shape: shape to match the data frame to
    """
    for i, df in enumerate(nfl_df_list):
        if df.shape == df_shape:
            return df
    if df_shape == (2, 6):
        return pd.DataFrame()
    else:
        return pd.DataFrame(pd.np.empty((0, 3)))


def get_data(url: str):
    """
    Getting the data from the website. The website will be collected as html and then passed into
    the pandas read_html to get the data tables from the html file.
    :param url: Website to collect the data from
    """
    try:
        html = requests.get(url).content
        nfl_df_list = pd.read_html(html)

        # checking if the first DataFrame from the url is empty
        # if the DataFrame at location 0 is empty return an empty DataFrame
        if nfl_df_list[0].empty or len(nfl_df_list) < 1:
            return pd.DataFrame
    except requests.ConnectionError as e:
        return handle_error(str(e.with_traceback(e.__traceback__)))
    except Exception as e:
        return handle_error(str(e.with_traceback(e.__traceback__)))

    # get the team data from the first pd.DataFrame
    team_df = get_df(nfl_df_list=nfl_df_list, df_shape=(2, 6))
    if team_df.empty:
        teams, points = ['NA', 'NA'], [-1, -1]
    else:
        teams, points = team_data(team_df)

    # getting the matchup data
    matchup_columns = np.append('game_stat', teams)
    matchup_df = get_df(nfl_df_list=nfl_df_list, df_shape=(25, 3))
    matchup_df.columns = matchup_columns

    # setting up points information and sending back the new DataFrame
    point_info = np.array([np.append('points', points)])
    matchup_df = matchup_df.append(pd.DataFrame(point_info, columns=matchup_columns))
    matchup_df.index = list(range(len(matchup_df.index)))

    return format_game_data_row(matchup_df)


def run(game_url: str):
    """
    Running the thread to call a url site and get the table data from the site.
    :param game_url: url list to make the url and index
    """
    url = make_url(game_url.split('/')[-1])
    cfb_log.info(str(url))
    new_data = get_data(url)
    rand_value = random.randrange(1, 4, 1)
    time.sleep(rand_value)
    cfb_log.info('******* {} *******'.format(rand_value))
    return new_data


def main_runner(game_urls: list):
    game_dfs = []

    # making a pool of processes to get the table data for each game
    with Pool(processes=10) as p:

        for df in p.map(run, game_urls):
            if not df.empty:
                game_dfs.append(df)

    # writing the data to a csv file
    final_df = pd.concat(game_dfs)
    final_df.index = list(range(final_df.shape[0]))
    cfb_log.info(str(final_df))
    file_path = str(Path(os.getcwd()+r'/final_df.csv'))
    cfb_log.info(file_path)
    final_df.to_csv(file_path, index=False)


# if __name__ == '__main__':
#     main()
