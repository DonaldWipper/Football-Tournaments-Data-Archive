from prefect import task, flow
import requests
from pathlib import Path
from sql import DBConnection
import pandas as pd
from io import StringIO
from multiprocessing.pool import ThreadPool

from utils import (
    transform,
    unix_timestamp_to_date,
    unix_timestamp_to_year,
    extract_add_info_from_path,
)

SOURCE = "clubelo"
S3_PATH_COMMON = f"{SOURCE}/football"
SCHEMA = "clubelo_staging"

from prefect_aws import S3Bucket, AwsCredentials

aws_credentials_block = AwsCredentials.load("yandex-object-storage")


@task(retries=1)
def write_local_common(df: pd.DataFrame,
                       club: str,
                       country: str,
                       date: str,
                       entity_type: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"{S3_PATH_COMMON}/{country}/{club}/").mkdir(parents=True, exist_ok=True)

    path = Path(f"{S3_PATH_COMMON}/{country}/{club}/{entity_type}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


def get_df_from_paths(entity_name: str, paths: list) -> pd.DataFrame:
    df = pd.concat((pd.read_parquet(f, engine='pyarrow').assign(path=str(f)) for f in paths))
    return df


def load_to_s3(path: Path) -> None:
    bucket_name = "euro-stat"
    s3 = aws_credentials_block.get_client("s3")
    s3.upload_file(str(path), bucket_name, str(path))
    print(f"File uploaded to S3: {bucket_name}/{str(path)}")


@flow()
def extract_from_ya_s3(
        date: str = None,
        entity_name: str = None
) -> list[Path]:
    import re

    bucket_name = "euro-stat"

    # Initialize S3 client
    s3 = aws_credentials_block.get_client("s3")

    s3_prefix = f"{SOURCE}/football/{entity_name}.parquet"

    files = []

    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucket_name, Prefix=s3_prefix, PaginationConfig={"MaxKeys": 1000}
    )

    for page in page_iterator:
        for obj in page.get("Contents", []):
            files.append(obj["Key"])

    return files


def write_mysql(df: pd.DataFrame, entity_type: str) -> None:
    print(df)
    df.to_sql(
        schema=SCHEMA,
        name=entity_type,
        con=DBConnection.get_engine(),
        if_exists="append",
        index=False
    )


@flow()
def etl_s3_to_mysql(
        entity_type: str = None,
        date: str = None,
) -> None:
    """Main ETL flow to load data into Big Query"""
    paths = extract_from_ya_s3(entity_type, date)
    print(paths)
    df = get_df_from_paths(entity_type, paths)
    df = transform(df)
    write_mysql(df, entity_type)


@flow()
def etl_web_to_ya_s3(
        date: str = None
):
    r = requests.get(f"http://api.clubelo.com/{date}")
    data = StringIO(r.text)
    df = pd.read_csv(data, sep=",")
    countries = set(df['Country'])
    print(countries)
    for country in countries:
        df_clubs = df[df['Country'] == country]
        clubs = set(df_clubs['Club'])

        print('here')
        # вызываем функцию загрузки для каждого файла
        for club in clubs:
            df_club = df_clubs[df_clubs['Club'] == club]
            path = write_local_common(df_club, club, country, "rating")
            load_to_s3(path)


if __name__ == "__main__":
    etl_web_to_ya_s3(date="2023-05-14")
    # etl_s3_to_mysql(entity_type="rating", date="2023-05-14")
