import re
import pandas as pd 

def read_data() -> pd.DataFrame:
    """ Function to return the read in data frame from the final_df. """
    return pd.read_csv('../data/final_df.csv')

if __name__ == '__main__':
    print(read_data().head())

