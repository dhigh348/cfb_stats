import re
import pandas as pd
import numpy as np
from functools import wraps


def high_order_data_transform(func):
    @wraps(func)
    def wrapper(*args, data: pd.DataFrame):
        new_data = data.copy()
        results = func(*args, data=new_data)
        return results
    return wrapper


def string_splitter(text: str, comp: str, location: int) -> str:
    """ Function to split the string. """
    if text == comp:
        return '0'
    return text.split('-')[location]


@high_order_data_transform
def add_dashed_cols(col: str, data: pd.DataFrame) -> pd.DataFrame:
    """ Function to return a DataFrame and get the new columns. """
    new_col_names = [col+'first_number', col+'second_number']

    temp_data = data[col].to_numpy(dtype=str)
    first_entry = np.vectorize(string_splitter)(temp_data, 0)
    second_entry = np.vectorize(string_splitter)(temp_data, 1)

    data[new_col_names[0]] = first_entry
    data[new_col_names[1]] = second_entry

    return data.drop([col], axis=1)


@high_order_data_transform
def rename_cols(regex_str: str, data: pd.DataFrame) -> pd.DataFrame:
    """ Removing certain characters from the column names. """
    data.columns = ['_'.join(re.sub(regex_str, ' ', name).split()) for name in data.columns]
    return data


@high_order_data_transform
def drop_funcs(cols: list, data: pd.DataFrame) -> pd.DataFrame:
    """ Function to drop columns and na rows. """
    data.drop(cols, axis=1, inplace=True)
    data.dropna(inplace=True)
    return data


def contains_dash(col: str, data: pd.DataFrame) -> bool:
    """ Checking if the column is of the dashed format. """
    return '-' in str(data.at[1, col])


def split_and_divide(value: str) -> float:
    """ Splitting the value and then returning the divided numbers. """
    value = value.split('-')
    if 'nan' in value or value[1] == '0':
        return float(0.0)
    x, y = value
    return float(x) / float(y)


def make_percentage(col_value) -> float:
    """ Taking a value from the series and returning the percentage from the string. """
    return col_value.map(split_and_divide)


def make_dashed_cols(data: pd.DataFrame) -> pd.DataFrame:
    """ Mapping over each column that contains dashes and creating new columns of the separate values. """
    cols = [c for c in data.columns if contains_dash(str(c), data)]
    data[cols] = data[cols].apply(make_percentage)
    return data


def total_time(possession_time: str) -> int:
    """ Taking the datetime string and returning the total int time of the team possession. """
    minutes, seconds = possession_time.split(':')
    return int(minutes) * 60 + int(seconds)

def apply_col_funcs(base_list: list, data: pd.DataFrame, funcs: list) -> pd.DataFrame:
    """ Mapping a function over the desired columns and transforming the data. """
    new_data = data.copy()
    for i, col in enumerate(base_list):
        new_data[col] = new_data[col].map(funcs[int(i/2)])
    return new_data


def create_labels(cols: list, data: pd.DataFrame) -> pd.DataFrame:
    """ Creating the labels for the DataFrame and data. """
    new_data = data.copy()
    new_data['home_wins'] = data[cols[0]].ge(data[cols[1]]).map(lambda x: 1 if x else 0)
    return new_data.drop(cols, axis=1)


if __name__ == '__main__':
    data = pd.read_csv('../data/final_df.csv')
    data = rename_cols('[()-_/:]', data=data)
    data = drop_funcs(['nnamed'], data=data)
    data = make_dashed_cols(data)

    team_dictionary = {team: i for i, team in enumerate(data['away_team'].unique())}
    team_dictionary['LAR'] = team_dictionary['STL']
    team_dictionary['LAC'] = team_dictionary['SD']

    data = apply_col_funcs(
        base_list=[h + '_possession' if i < 2 else h + '_team' for i, h in enumerate(['home', 'away', 'home', 'away'])],
        data=data,
        funcs=[total_time, lambda x: team_dictionary[x]]
    )
    data = create_labels(['home_points', 'away_points'], data)

    data.to_csv('../data/cleaned_data.csv', index=False)

