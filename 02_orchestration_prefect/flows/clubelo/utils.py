import json
import pandas as pd
import numpy as np
import datetime
from prefect import task
from pathlib import Path


def unix_timestamp_to_date(timestamp: int) -> str:
    return datetime.datetime.fromtimestamp(timestamp).date().strftime('%Y-%m-%d')


def unix_timestamp_to_year(timestamp: int) -> str:
    return datetime.datetime.fromtimestamp(timestamp).year


def str_to_int(row):
    # row = str(row).replace('"', '')
    if (row is None) or (row == ""):
        return 0
    row = int(row)
    return row


def str_to_float(row):
    if (row is None) or (row == ""):
        return 0
    row = str(row).replace(",", ".").replace(u'\xa0', "")
    return row


# def extract_add_info_from_path(path: str) -> tuple:
#     # define the regular expression pattern
#     import re
#
#     match = re.match(
#         r"sports\.ru/football/(?P<tournament_id>\d+)/(?P<year>\d+)/(?P<stage_name>[^/]+)/(?P<date>\d{4}-\d{2}-\d{2})",
#         path)
#     tournament_id = match.group("tournament_id")
#     year = match.group("year")
#     stage_name = match.group("stage_name")
#     date = match.group("date")
#     return year, date, stage_name, tournament_id


def extract_add_info_from_path(path: str) -> tuple:
    # define the regular expression pattern
    import re

    match = re.match(
        r"uefa/football/tournament_id=(?P<tournament_id>\d+)/year=(?P<year>\d+)/stage_name=(?P<stage_name>[^/]+)/date=(?P<date>\d{8})",
        path)
    tournament_id = match.group("tournament_id")
    year = match.group("year")
    stage_name = match.group("stage_name")
    date = match.group("date")
    return year, date, stage_name, tournament_id


class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """

    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32,
                              np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def get_mysql_fields_config():
    '''Returns config for Mysql columns\'s datatypes'''
    with open('mysql_types.json') as input_file:
        mysql_types_json = json.loads(input_file.read())
    return mysql_types_json


@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Data cleaning example"""
    # [extract_add_info_from_path(str(f)) for f in paths]

    # df = pd.concat((pd.read_parquet(f, engine='pyarrow').assign(path=str(f)) for f in paths))
    #
    # df['path_info'] = df['path'].apply(extract_add_info_from_path)
    # df[['year', 'date', 'stage_name', 'tournament_id']] = pd.DataFrame(df['path_info'].tolist(), index=df.index)
    # df.drop(columns=['path', 'path_info'], inplace=True)

    conf = get_mysql_fields_config()

    for key in list(df.columns):
        field_type = conf.get(key, "json")

        if field_type == "json":
            try:
                df[key] = df[key].fillna('')
                df[key] = df.apply(lambda x: json.dumps(x[key], cls=NumpyEncoder), axis=1)
            except TypeError as E:
                raise TypeError(str(E) + " " + key)
        elif field_type == "int":
            df[key] = df[key].fillna(0)
            df[key] = df.apply(
                lambda x: str_to_int(x[key]), axis=1
            ).astype(int)
        elif field_type == "float":
            df[key] = df[key].fillna(0)
            df[key] = df.apply(
                lambda x: str_to_float(x[key]), axis=1
            ).astype(float)
        elif field_type == "string":
            df[key] = df[key].astype(str)
        elif field_type == "bool":
            df[key] = df[key].astype(bool)
        elif field_type == "date":
            df[key] = pd.to_datetime(df[key], format='%Y-%m-%d')
        else:
            try:
                df[key] = df[key].fillna('')
                df[key] = df.apply(lambda x: json.dumps(x[key], cls=NumpyEncoder), axis=1)
            except TypeError as E:
                raise TypeError(str(E) + " " + key)
    return df
