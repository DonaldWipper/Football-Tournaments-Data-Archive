import pandas as pd
import datetime
import boto3
from pathlib import Path
from prefect import task, flow
from enum import Enum
from prefect.filesystems import S3

from sports_api import Sportsapi
from utils import transform, unix_timestamp_to_date, unix_timestamp_to_year, extract_add_info_from_path
from sql import DBConnection

from prefect_aws import S3Bucket, AwsCredentials

aws_credentials_block = AwsCredentials.load("yandex-object-storage")

S3_PATH_DEEP = "sports.ru/football/{tournament_id}/{year}/{stage_name}/{date}"
S3_PATH_COMMON = "sports.ru/football"


@task(retries=1)
def write_local_common(df: pd.DataFrame, entity_type: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(S3_PATH_COMMON).mkdir(parents=True, exist_ok=True)

    path = Path(f"{S3_PATH_COMMON}/{entity_type}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(retries=1)
def write_local_deep(df: pd.DataFrame,
                     entity_type: str,
                     tournament_id: int,
                     year: int,
                     stage_name: str,
                     date: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = S3_PATH_DEEP.format(tournament_id=tournament_id, year=year, stage_name=stage_name, date=date)
    Path(path).mkdir(parents=True, exist_ok=True)

    path = Path(f"{path}/{entity_type}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(retries=1)
def get_all_seasons(tournament_id: int = None) -> list:
    tournaments = Sportsapi.get_all_tournaments()

    if tournament_id:
        relevant_tournaments = [tournament_id]
    else:
        relevant_tournaments = [t for t in tournaments['id']]

    # Find the most recent season for each relevant tournament
    seasons = []
    for t in relevant_tournaments:
        seasons_df = Sportsapi.get_stat_seasons_by_id(t)
        if 'name' in seasons_df.columns:
            seasons_df['tournament_id'] = t
            seasons_ = seasons_df.sort_values(['name'], ascending=[0]).to_dict('records')
            print(seasons_)
            seasons += seasons_
    return seasons


def write_mysql(df: pd.DataFrame, entity_name: str) -> None:
    print(df)
    df.to_sql(
        name=entity_name, con=DBConnection.get_engine(), if_exists="append", index=False
    )


def retrieve_seasons() -> list:
    data = DBConnection.get_dict_from_table('tournament_stat_seasons_s3', condition={"is_on_s3": 0})
    return data


@flow()
def etl_web_to_ya_s3(entity_type: str, tournament_id: int = None) -> None:
    """Load data from api to object storage"""
    if entity_type == "tournaments":
        df = Sportsapi.get_all_tournaments()
    elif entity_type == "tournament_stat_seasons":
        df = pd.DataFrame(data=get_all_seasons())

    if entity_type in ["tournaments", "tournament_stat_seasons"]:
        path = write_local_common(df, entity_type)
        load_to_s3(path)

    if entity_type == "tournament_calendar":
        last_seasons = get_all_seasons(tournament_id)
        for season in last_seasons:
            calendar_ = Sportsapi.get_tour_calendar(tournament_id=season['tournament_id'], season_id=season['id'])

            for item in calendar_:
                stage_name = item['stage_name'] if item['stage_name'] != "" else "no_stage_name"
                matches = item["matches"]

                dates = sorted(list(set([unix_timestamp_to_date(v['time']) for v in matches])), reverse=True)
                for date in dates:
                    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
                    year = dt.year
                    matches_ = [m for m in matches if unix_timestamp_to_date(m['time']) == date]
                    print(date, len(matches_))
                    df = pd.DataFrame(data=matches_)
                    path = write_local_deep(df, entity_type, season['tournament_id'], year, stage_name, date)
                    load_to_s3(path)
                    # DBConnection.execute(f"""update sports_ru_staging.tournament_stat_seasons_s3
                    #                          set is_on_s3 = 1
                    #                          where id = {season['id']}""")


class SearchOption(Enum):
    ALL = 1
    TOURNAMENT_YEAR = 2
    TOURNAMENT_YEAR_DATE = 3


def extract_from_ya_s3(tournament_id: int = None,
                       year: int = None,
                       entity_name: str = None,
                       date: str = None,
                       search_option: SearchOption = SearchOption.ALL
                       ) -> list[Path]:
    import re

    bucket_name = 'euro-stat'

    # Initialize S3 client
    s3 = aws_credentials_block.get_client('s3')

    s3_prefix = f"sports.ru/football/"

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

    pattern_date = re.compile(f".*{date}.*")
    pattern_tournament = re.compile(f".*/{tournament_id}/.*")
    pattern_year = re.compile(f".*/{year}/.*")

    files = []
    print('here')
    for obj in response.get('Contents', []):
        if search_option == SearchOption.ALL:
            if entity_name in obj['Key']:
                if date and pattern_date.match(obj['Key']):
                    files.append(obj['Key'])
                elif year and pattern_year.match(obj['Key']):
                    files.append(obj['Key'])
                elif tournament_id and pattern_tournament.match(obj['Key']):
                    files.append(obj['Key'])
                elif not date and not year and not tournament_id:
                    files.append(obj['Key'])
        elif search_option == SearchOption.TOURNAMENT_YEAR:
            if tournament_id and year:
                if entity_name in obj['Key'] and f"{tournament_id}/{year}/" in obj['Key']:
                    if date and pattern_date.match(obj['Key']):
                        files.append(obj['Key'])
                    elif not date:
                        files.append(obj['Key'])
        elif search_option == SearchOption.TOURNAMENT_YEAR_DATE:
            if tournament_id and year and date:
                if entity_name in obj['Key'] and f"{tournament_id}/{year}/{date}" in obj['Key']:
                    files.append(obj['Key'])

    print('here2')
    paths = []
    for file_path in files:
        Path(file_path.replace(f'{entity_name}.parquet', '')).mkdir(parents=True, exist_ok=True)
        path = Path(file_path.replace(f'{entity_name}.parquet', ''))
        paths.append(path)
        # s3.download_file(bucket_name, file_path, file_path)

    return paths


def etl_s3_to_mysql(tournament_id: int = None,
                    year: int = None,
                    entity_name: str = None,
                    date: str = None) -> None:
    """Main ETL flow to load data into Big Query"""
    paths = extract_from_ya_s3(tournament_id, year, entity_name, date)
    df = transform(paths)
    write_mysql(df)


def load_to_s3(path: Path) -> None:
    bucket_name = 'euro-stat'
    s3 = aws_credentials_block.get_client('s3')
    s3.upload_file(str(path), bucket_name, str(path))
    print(f"File uploaded to S3: {bucket_name}/{str(path)}")


if __name__ == "__main__":
    etl_web_to_ya_s3(entity_type='tournament_calendar', tournament_id=52)
    # entities = ["tournaments", "tournament_stat_seasons"]
    # for entity in entities:
    #     etl_web_to_ya_s3(entity)
