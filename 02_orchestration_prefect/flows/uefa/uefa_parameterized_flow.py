import pandas as pd
import datetime
from pathlib import Path
from prefect import task, flow
from enum import Enum

from uefa_api import UefaApi
from utils import (
    transform,
    unix_timestamp_to_date,
    unix_timestamp_to_year,
    extract_add_info_from_path,
)
from sql import DBConnection

from prefect_aws import S3Bucket, AwsCredentials

aws_credentials_block = AwsCredentials.load("yandex-object-storage")

SOURCE = "uefa"
S3_PATH_DEEP = "uefa/football/tournament_id={tournament_id}/year={year}/stage_name={stage_name}/date={date}"
S3_PATH_DEEP_MATCH = "uefa/football/tournament_id={tournament_id}/year={year}/stage_name={stage_name}/date={date}/match_id={match_id}"
S3_PATH_COMMON = "uefa/football"


@task(retries=1)
def write_local_common(df: pd.DataFrame, entity_type: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(S3_PATH_COMMON).mkdir(parents=True, exist_ok=True)

    path = Path(f"{S3_PATH_COMMON}/{entity_type}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(retries=1)
def write_local_deep(
        df: pd.DataFrame,
        entity_type: str,
        tournament_id: int,
        year: int,
        stage_name: str,
        date: str,
) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = S3_PATH_DEEP.format(
        tournament_id=tournament_id, year=year, stage_name=stage_name, date=date
    )
    Path(path).mkdir(parents=True, exist_ok=True)

    path = Path(f"{path}/{entity_type}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(retries=1)
def write_local_deep_match(
        df: pd.DataFrame,
        entity_type: str,
        tournament_id: int,
        year: int,
        stage_name: str,
        date: str,
        match_id: int
) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = S3_PATH_DEEP_MATCH.format(
        tournament_id=tournament_id, year=year, stage_name=stage_name, date=date, match_id=match_id
    )
    Path(path).mkdir(parents=True, exist_ok=True)

    path = Path(f"{path}/{entity_type}.parquet")
    df.to_parquet(path, compression="gzip")
    return path



def write_mysql(df: pd.DataFrame, entity_type: str) -> None:
    print(df)
    df.to_sql(
        name=entity_type, con=DBConnection.get_engine(), if_exists="append", index=False
    )


@flow()
def etl_web_to_ya_s3(
        entity_type: str,
        competition_id: int = None,
        year: int = None
) -> None:
    """Load data from api to object storage"""
    if entity_type == "competitions":
        df = UefaApi.get_competitions()
    elif entity_type == "matches":
        df = UefaApi.get_matches(competition_id, year)

    if entity_type in ["competitions"]:
        path = write_local_common(df, entity_type)
        load_to_s3(path)

    # if entity_type == "tournament_calendar":
    #     if date_int:
    #         year = datetime.datetime.strptime(str(date_int), "%Y%m%d").year
    #         last_seasons = get_all_seasons(tournament_id, year)
    #     else:
    #         last_seasons = get_all_seasons(tournament_id)
    #
    #     for season in last_seasons:
    #         calendar_ = Sportsapi.get_tour_calendar(
    #             tournament_id=season["tournament_id"], season_id=season["id"]
    #         )
    #
    #         for item in calendar_:
    #             stage_name = (
    #                 item["stage_name"] if item["stage_name"] != "" else "no_stage_name"
    #             )
    #             matches = item["matches"]
    #
    #             dates = sorted(
    #                 list(set([unix_timestamp_to_date(v["time"]) for v in matches])),
    #                 reverse=True,
    #             )
    #
    #             if date_int:
    #                 dates = [
    #                     d
    #                     for d in dates
    #                     if datetime.datetime.strptime(d, "%Y-%m-%d").strftime(
    #                         "%Y%m%d"
    #                     )
    #                        == str(date_int)
    #                 ]
    #             for date in dates:
    #                 dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    #                 date_int = dt.strftime("%Y%m%d")
    #                 year = dt.year
    #                 matches_ = [
    #                     m for m in matches if unix_timestamp_to_date(m["time"]) == date
    #                 ]
    #                 matches_ids = [m['id'] for m in matches_]
    #                 matches_stat = Sportsapi.get_matches_by_tournament_and_day(
    #                     tournament_id=season["tournament_id"], date=int(date_int)
    #                 )
    #                 print(date, len(matches_))
    #
    #                 df = pd.DataFrame(data=matches_)
    #                 df_matches_stat = pd.DataFrame(data=matches_stat)
    #
    #                 df_matches_stat["tournament_id"] = df["tournament_id"] = season[
    #                     "tournament_id"
    #                 ]
    #                 df_matches_stat["year"] = df["year"] = year
    #                 df_matches_stat["stage_name"] = df["stage_name"] = stage_name
    #                 df_matches_stat["date"] = df["date"] = date
    #
    #                 path = write_local_deep(
    #                     df,
    #                     entity_type,
    #                     season["tournament_id"],
    #                     year,
    #                     stage_name,
    #                     date_int,
    #                 )
    #                 load_to_s3(path)
    #
    #                 path = write_local_deep(
    #                     df=df_matches_stat,
    #                     entity_type="matches",
    #                     tournament_id=season["tournament_id"],
    #                     year=year,
    #                     stage_name=stage_name,
    #                     date=date_int,
    #                 )
    #                 load_to_s3(path)
    #
    #                 for match_id in matches_ids:
    #                     df_match_stat = pd.DataFrame(data=Sportsapi.get_match_stat_by_id(match_id))
    #                     df_match_stat['command1'] = df_match_stat['command1'].astype(str)
    #                     df_match_stat['command2'] = df_match_stat['command2'].astype(str)
    #                     path = write_local_deep_match(
    #                         df=df_match_stat,
    #                         entity_type="match_stat",
    #                         tournament_id=season["tournament_id"],
    #                         year=year,
    #                         stage_name=stage_name,
    #                         date=date_int,
    #                         match_id=match_id
    #                     )
    #                     load_to_s3(path)
    #
    #                 # DBConnection.execute(f"""update sports_ru_staging.tournament_stat_seasons_s3
    #                 #                          set is_on_s3 = 1
    #                 #                          where id = {season['id']}""")


class SearchOption(Enum):
    ALL = 1
    TOURNAMENT_YEAR = 2
    TOURNAMENT_YEAR_DATE = 3


def extract_from_ya_s3(
        tournament_id: int = None,
        year: int = None,
        entity_type: str = None,
        date: str = None,
        search_option: SearchOption = SearchOption.ALL,
) -> list[Path]:
    import re

    bucket_name = "euro-stat"

    # Initialize S3 client
    s3 = aws_credentials_block.get_client("s3")

    s3_prefix = f"sports.ru/football/"

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

    pattern_date = re.compile(f".*{date}.*")
    pattern_tournament = re.compile(f".*/{tournament_id}/.*")
    pattern_year = re.compile(f".*/{year}/.*")

    files = []

    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucket_name, Prefix=s3_prefix, PaginationConfig={"MaxKeys": 1000}
    )

    for page in page_iterator:
        for obj in page.get("Contents", []):

            if search_option == SearchOption.ALL:

                if entity_type in obj["Key"]:
                    if date and pattern_date.match(obj["Key"]):
                        files.append(obj["Key"])
                    elif year and pattern_year.match(obj["Key"]):
                        files.append(obj["Key"])
                    elif tournament_id and pattern_tournament.match(obj["Key"]):

                        files.append(obj["Key"])
                    elif not date and not year and not tournament_id:
                        files.append(obj["Key"])
            elif search_option == SearchOption.TOURNAMENT_YEAR:
                if tournament_id and year:
                    if (
                            entity_type in obj["Key"]
                            and f"{tournament_id}/{year}/" in obj["Key"]
                    ):
                        if date and pattern_date.match(obj["Key"]):
                            files.append(obj["Key"])
                        elif not date:
                            files.append(obj["Key"])
            elif search_option == SearchOption.TOURNAMENT_YEAR_DATE:
                if tournament_id and year and date:
                    if (
                            entity_type in obj["Key"]
                            and f"{tournament_id}/{year}/{date}" in obj["Key"]
                    ):
                        files.append(obj["Key"])

    paths = []
    for file_path in files:
        Path(file_path.replace(f"{entity_type}.parquet", "")).mkdir(
            parents=True, exist_ok=True
        )
        path = Path(file_path.replace(f"{entity_type}.parquet", ""))
        paths.append(path)
        # s3.download_file(bucket_name, file_path, file_path)

    return paths


def etl_s3_to_mysql(
        tournament_id: int = None,
        year: int = None,
        entity_type: str = None,
        date: str = None,
) -> None:
    """Main ETL flow to load data into Big Query"""
    paths = extract_from_ya_s3(tournament_id, year, entity_type, date)
    df = transform(paths)
    write_mysql(df)


def load_to_s3(path: Path) -> None:
    bucket_name = "euro-stat"
    s3 = aws_credentials_block.get_client("s3")
    s3.upload_file(str(path), bucket_name, str(path))
    print(f"File uploaded to S3: {bucket_name}/{str(path)}")


if __name__ == "__main__":
    # paths = extract_from_ya_s3(entity_type='tournament_calendar', tournament_id=52)
    # df = pd.concat((pd.read_parquet(f, engine='pyarrow').assign(path=str(f)) for f in paths))
    # print(json.dumps(dict(df['command2'][0:1])))
    etl_web_to_ya_s3(entity_type="competitions")
    etl_web_to_ya_s3(entity_type="matches", competion_id=3, year=2020)
    # entities = ["tournaments", "tournament_stat_seasons"]
    # for entity in entities:
    #     etl_web_to_ya_s3(entity)
