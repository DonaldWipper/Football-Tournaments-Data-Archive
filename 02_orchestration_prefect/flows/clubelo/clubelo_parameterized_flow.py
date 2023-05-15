from prefect import task, flow
import requests
from pathlib import Path
import pandas as pd
from io import StringIO

S3_PATH_COMMON = "sports.ru/football"
SOURCE = "uefa"
SCHEMA = "clubelo_staging"

from prefect_aws import S3Bucket, AwsCredentials

aws_credentials_block = AwsCredentials.load("yandex-object-storage")


@task(retries=1)
def write_local_common(df: pd.DataFrame, entity_type: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(S3_PATH_COMMON).mkdir(parents=True, exist_ok=True)

    path = Path(f"{S3_PATH_COMMON}/{entity_type}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


def load_to_s3(path: Path) -> None:
    bucket_name = "euro-stat"
    s3 = aws_credentials_block.get_client("s3")
    s3.upload_file(str(path), bucket_name, str(path))
    print(f"File uploaded to S3: {bucket_name}/{str(path)}")


@flow()
def etl_web_to_ya_s3(
        date: str = None
):
    r = requests.get(f"http://api.clubelo.com/{date}")
    data = StringIO(r.text)
    df = pd.read_csv(data, sep=",")
    path = write_local_common(df, "rating")
    load_to_s3(path)


if __name__ == "__main__":

    etl_web_to_ya_s3(date="2023-05-14")


