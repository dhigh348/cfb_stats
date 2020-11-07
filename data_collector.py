import pandas as pd
import numpy as np
import requests
import os

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


def make_url(page_number: int):
    """
    Making the url to lookup the data.
    """
    return 'https://www.espn.com/nfl/matchup?gameId={}'.format(page_number)


def get_data(url: str):
    """
    Getting the data from the website. The website will be collected as html and then passed into
    the pandas read_html to get the data tables from the html file.
    :param url: Website to collect the data from
    """
    try:
        html = requests.get(url).content
        nfl_df_list = pd.read_html(html)

        # printing the DataFrames from the url website
        for i, df in enumerate(nfl_df_list):
            cfb_log.info('{}: \n {}'.format(i, str(df.tail())))

        # checking if the first DataFrame from the url is empty
        # if the DataFrame at location 0 is empty return an empty DataFrame
        if nfl_df_list[0].empty:
            return pd.DataFrame
    except Exception as e:
        cfb_log.info(str(e.with_traceback(e.__traceback__)))
        return pd.DataFrame

    # get the team data from the first pd.DataFrame
    team_df = nfl_df_list[0]
    teams, points = team_data(team_df)

    # getting the matchup data
    matchup_columns = np.append('game_stat', teams)
    matchup_df = nfl_df_list[1]
    matchup_df.columns = matchup_columns

    # setting up points information and sending back the new DataFrame
    point_info = np.array([np.append('points', points)])
    matchup_df = matchup_df.append(pd.DataFrame(point_info, columns=matchup_columns))
    matchup_df.index = list(range(len(matchup_df.index)))

    return format_game_data_row(matchup_df)


def run(value: int):
    """
    Running the thread to call a url site and get the table data from the site.
    :param value: integer that indicates the game number
    """
    url = make_url(value)
    cfb_log.info(str(url))
    new_data = get_data(url)
    return new_data


def main():
    starting_point = 220000000
    count = 100000
    game_dfs = []

    # going through each of the possible url beginnings
    for i in range(210):

        # going through each of the 100000 calls for each thread to get the game data
        with Pool(processes=10) as p:
            starting_point = starting_point + (i * count)

            for df in p.map(run, range(starting_point, starting_point + count)):
                # checking if the returned DataFrame is empty upon return
                if not df.empty:
                    game_dfs.append(df)
                    cfb_log.info(str(game_dfs[-1].tail()))

    # writing the data to a csv file
    final_df = pd.concat(game_dfs)
    final_df.index = list(range(final_df.shape[0]))
    cfb_log.info(str(final_df))
    file_path = str(Path(os.getcwd()+r'/final_df.csv'))
    cfb_log.info(file_path)
    final_df.to_csv(file_path, index=True)


if __name__ == '__main__':
    main()
