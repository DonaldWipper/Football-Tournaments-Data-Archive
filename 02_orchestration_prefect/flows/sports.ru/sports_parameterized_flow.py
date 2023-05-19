import pandas as pd
from prefect import task, flow
from pyspark import SparkConf
from multiprocessing import cpu_count
from sports_api import Sportsapi
from pyspark.sql import SparkSession
from prefect_aws import S3Bucket, AwsCredentials

from prefect import task, flow
import requests
import contextlib
from pathlib import Path
import pandas as pd
from io import StringIO
from multiprocessing import cpu_count
from pyspark import SparkConf
from multiprocessing.pool import ThreadPool

from pyspark.sql.functions import col

aws_credentials_block = AwsCredentials.load("yandex-object-storage")

S3_PATH_DEEP = "sports.ru/football/tournament_id={tournament_id}/year={year}/stage_name={stage_name}/date={date}"
S3_PATH_DEEP_MATCH = "sports.ru/football/tournament_id={tournament_id}/year={year}/stage_name={stage_name}/date={date}/match_id={match_id}"
S3_PATH_COMMON = "sports.ru/football"


@task(retries=1)
def get_all_seasons(tournament_id: int = None, year: int = None) -> list:
    tournaments = Sportsapi.get_all_tournaments()

    if tournament_id:
        relevant_tournaments = [tournament_id]
    else:
        relevant_tournaments = [t for t in tournaments["id"]]

    # Find the most recent season for each relevant tournament
    seasons = []
    tasks = []

    for t in relevant_tournaments:
        seasons_df = Sportsapi.get_stat_seasons_by_id(t)
        if "name" in seasons_df.columns:
            if year:
                seasons_df = seasons_df[seasons_df['name'].str.contains(str(year))]
            seasons_df["tournament_id"] = t
            seasons_ = seasons_df.sort_values(["name"], ascending=[0]).to_dict(
                "records"
            )

            print(seasons_)
            seasons += seasons_

    return seasons


def local_to_s3(local_dir: str):
    import subprocess
    s3_bucket = 's3://euro-stat/'

    # aws s3 cp --recursive --exclude "*" --include "*.parquet" /path/to/local_dir s3://s3_bucket/s3_prefix/
    subprocess.run('source /home/jovyan/work/_credentials', shell=True)
    command = ['aws', 's3', 'cp', '--recursive', '--exclude', ' "*" ', '--include', ' "*.parquet" ', local_dir,
               s3_bucket + local_dir, "--endpoint-url", "https://storage.yandexcloud.net"]
    print(" ".join(command))
    # subprocess.run(command, check=True)




@task
def extract_from_ya_s3(spark: SparkSession, entity_name: str = None):
    # Создаем SparkSession

    r = requests.get(f"http://api.clubelo.com/{date}")
    data = StringIO(r.text)

    df = pd.read_csv(data, sep=",")
    import datetime

    df["year"] = datetime.datetime.strptime(date, "%Y-%m-%d").year
    df["date"] = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d")
    df["entity"] = "rating"
    df = df.rename(columns={"Country": "country", "Club": "club"}, errors="raise")

    # Загружаем данные из файла CSV в DataFrame
    df_spark = spark.createDataFrame(df)

    partitioned_df = df_spark.repartition(
        col("year"), col("country"), col("club"), col("date"), col("entity")
    )

    # Сохранить каждый файл в формате parquet локально
    partitioned_df.write.partitionBy("year", "country", "club", "date", "entity").mode(
        "overwrite"
    ).parquet(S3_PATH_COMMON)

    local_to_s3(S3_PATH_COMMON)


@flow()
def etl_web_to_ya_s3(
        entity_type: str,
        tournament_id: int = None,
        year: int = None,
        date_int: int = None
) -> None:
    """Load data from api to object storage"""
    if entity_type == "tournaments":
        df = Sportsapi.get_all_tournaments()
    elif entity_type == "tournament_stat_seasons":
        df = pd.DataFrame(data=get_all_seasons())

    if entity_type in ["tournaments", "tournament_stat_seasons"]:
        path = write_local_common(df, entity_type)
        load_to_s3(path)

    if entity_type == "tournament_calendar":
        if date_int:

# We assume that you have added your credential with $ aws configure
def get_aws_credentials():
    with open(os.path.expanduser("~/.aws/credentials")) as f:
        for line in f:
            # print(line.strip().split(' = '))
            try:
                key, val = line.strip().split(' = ')
                if key == 'AWS_ACCESS_KEY_ID':
                    aws_access_key_id = val
                elif key == 'AWS_SECRET_ACCESS_KEY':
                    aws_secret_access_key = val
            except ValueError:
                pass
    return aws_access_key_id, aws_secret_access_key


@contextlib.contextmanager
def get_spark_session(conf: SparkConf):
    """
    Function that is wrapped by context manager
    Args:
      - conf(SparkConf): It is the configuration for the Spark session
    """

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext

    access_key, secret_key = get_aws_credentials()

    # remove this block if use core-site.xml and env variable
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://storage.yandexcloud.net")
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3.access.key", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")

    try:
        yield spark
    finally:
        spark.stop()



@flow(name="sports.ru pipeline flow")
def data_pipeline(entity_type: str, tournament_id: int = None, year:int, date_int: int = None):
    '''
    Function wrapped to be used as the Prefect main flow
    '''

    n_cpus = cpu_count()
    n_executors = n_cpus - 1
    n_cores = 4
    n_max_cores = n_executors * n_cores
    conf = SparkConf().setAppName("clubelo")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.driver.memory", "10g")
    conf.set("spark.executor.cores", str(n_cores))
    conf.set("spark.cores.max", str(n_max_cores))
    with get_spark_session(conf=conf) as spark_session:
        extract_from_ya_s3(spark_session, entity_type, tournament_id, date_int)


if __name__ == "__main__":
    # paths = extract_from_ya_s3(entity_type='tournament_calendar', tournament_id=52)
    # df = pd.concat((pd.read_parquet(f, engine='pyarrow').assign(path=str(f)) for f in paths))
    # print(json.dumps(dict(df['command2'][0:1])))
    etl_web_to_ya_s3(entity_type="tournament_calendar", tournament_id=52)
    # entities = ["tournaments", "tournament_stat_seasons"]
    # for entity in entities:
    #     etl_web_to_ya_s3(entity)
