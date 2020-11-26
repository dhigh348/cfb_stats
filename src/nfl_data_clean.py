import re
import pandas as pd
import numpy as np


def string_splitter(text: str, comp: str, location: int) -> str:
    """ Function to split the string. """
    if text == comp:
        return '0'
    return text.split('-')[location]


def add_dashed_cols(col: str, data: pd.DataFrame) -> pd.DataFrame:
    """ Function to return a DataFrame and get the new columns. """
    new_col_names = [col+'first_number', col+'second_number']

    temp_data = data[col].to_numpy(dtype=str)
    first_entry = np.vectorize(string_splitter)(temp_data, 0)
    second_entry = np.vectorize(string_splitter)(temp_data, 1)

    new_data = data.copy()
    new_data[new_col_names[0]] = first_entry
    new_data[new_col_names[1]] = second_entry

    return new_data.drop([col], axis=1)


def rename_cols(regex_str: str, data: pd.DataFrame) -> pd.DataFrame:
    """ Removing certain characters from the column names. """
    new_data = data.copy()
    new_data.columns = ['_'.join(re.sub(regex_str, ' ', name).split()) for name in data.columns]
    return new_data


def drop_funcs(cols: list, data: pd.DataFrame) -> pd.DataFrame:
    """ Function to drop columns and na rows. """
    new_data = data.copy()
    new_data.drop(cols, axis=1, inplace=True)
    new_data.dropna(inplace=True)
    return new_data


if __name__ == '__main__':
    data = drop_funcs(['nnamed'], rename_cols('[()-_/:]', pd.read_csv('../data/final_df.csv')))
    print(data.head())

