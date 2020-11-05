import sys
import logging

import pandas as pd

from nfl_game_data_collector import GameData
from log_cfb import CFBDataLogger

from multiprocessing import Pool


def main(log: logging.Logger):
    starting_point = 220000000
    count = 100000
    game_threads = []
    game_dfs = []

    # going through each of the possible url beginnings
    for i in range(210):

        # going through each of the 100000 calls for each thread to get the game data
        with Pool(processes=10, ) as p:
            starting_point = starting_point + (i * count)
            game_data = p.imap_unordered(GameData(begin_point, count, log).start, range(1000000))
            game_threads.append(game_data)
            game_data.start()

        # appending the games to the data frame
        for game_data in game_threads:
            game_dfs.append(game_data.get_game_df())

    # writing the data to a csv file
    final_df = pd.concat(game_dfs)
    final_df.to_csv()


if __name__ == '__main__':
    cfb_log = CFBDataLogger()
    main(cfb_log)
