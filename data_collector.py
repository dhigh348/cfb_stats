import sys
import logging

from nfl_game_data_collector import GameData

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
    count = 130000
    game_threads = []
    game_dfs = []

    for i in range(21):
        begin_point = starting_point + i * 10000000
        game_data = GameData(begin_point, count, root) 
        game_threads.append(game_data)
        game_data.start()
  
    for game_thread in game_threads:
        game_thread.join()
    
    for game_data in game_threads:
        game_dfs.append(game_data.get_game_df())
  
    final_df = pd.concat(game_dfs)
    final_df.to_csv()


if __name__ == '__main__':
    main()