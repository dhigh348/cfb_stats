import sys
import logging

import pandas as pd

from nfl_game_data_collector import GameData

from multiprocessing import Pool


# setting up the logger for running the program
root = logging.getLogger() # setting the root logger
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout) # setting the handler
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s %(name)s - %(levelname)s: \n %(message)s\n') # formatter
handler.setFormatter(formatter)
root.addHandler(handler)


def main():
    starting_point = 220000000
    count = 100000
    game_threads = []
    game_dfs = []

    # going through each of the possible url beginnings
    for i in range(210):

        # going through each of the 100000 calls for each thread to get the game data
        for j in range(10):
            begin_point = starting_point + j * 1000000
            game_data = GameData(begin_point, count, root)
            game_threads.append(game_data)
            game_data.start()

        # joining the processes
        for game in game_threads:
            game.join()

        # appending the games to the data frame
        for game_data in game_threads:
            game_dfs.append(game_data.get_game_df())

        # clearing the threads to start new threads
        game_threads.clear()

    # writing the data to a csv file
    final_df = pd.concat(game_dfs)
    final_df.to_csv()


if __name__ == '__main__':
    main()
