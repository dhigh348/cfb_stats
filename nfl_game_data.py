import time 
import requests

import numpy as np
import pandas as pd
# import seaborn as sns


def team_data(df: pd.DataFrame):
	"""
	Transforming the team data frame into the data that contains the names of 
	the teams and the ending points of the game. 
	:param df: the data passed in 
	"""
	teams = df[df.columns[0]].tolist()
	points = df[df.columns[-1]].tolist()
	return teams, points


def format_game_data_row(df: pd.DataFrame):
	"""
	Transforming the data frame passed into a new dataframe containing one row 
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


def get_table_data(url: str):
	"""
	Function going to each link and getting the game data from the url. 
	Pandas will be used to get the table.
	:param url: string of the url to get
	"""
	try: 
		html = requests.get(url).content
		nfl_df_list = pd.read_html(html)
	except Exception as e:
		return pd.DataFrame() 

	# getting the team scores and the teams that played
	team_df = nfl_df_list[0]
	teams, points = team_data(team_df)

	# returning the matchup data
	matchup_columns = np.append('matchup', teams)
	matchup_df = nfl_df_list[1]
	matchup_df.columns = matchup_columns
	return format_game_data_row(matchup_df)


def main():

	# making the options for opening the webpage
	# options = option_setter(['--disable-notifications'])

	# creating the webdriver
	# run_driver(options=options)
	# https://www.espn.com/nfl/matchup?gameId=401127978
	page_name = 'https://www.espn.com/nfl/matchup?gameId='
	first_page = 320905019
	count = 100000000
	url_hit_list = []
	started = False

	for page in range(count):
		current_page = first_page + page 
		request_url = page_name + str(current_page)
		if not started:
			temp_df = get_table_data(request_url)
			if not temp_df.empty:
				started = True	
				url_hit_list.append(current_page)
				print('********************** {} ********************'.format(current_page))
				print(temp_df.tail())
		else:
			concat_df = get_table_data(request_url)
			if not concat_df.empty:
				url_hit_list.append(current_page)
				temp_df = pd.concat([temp_df, concat_df])
				temp_df.index = [i for i in range(len(temp_df.index))]
				print('********************** {} ********************'.format(current_page))
				print(temp_df.tail())

	temp_df.to_csv(index=False)
	print(url_hit_list)


if __name__ == '__main__':
	main()
























