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

SOURCE = "clubelo"
S3_PATH_COMMON = f"{SOURCE}/football"
S3_PATH_DEEP = (
    "sports.ru/football/year={year}/country={country}/club={club}/date={date}"
)
SCHEMA = "clubelo_staging"

from prefect_aws import S3Bucket, AwsCredentials

aws_credentials_block = AwsCredentials.load("yandex-object-storage")

from prefect import task
from pyspark.sql import SparkSession
import os


# We assume that you have added your credential with $ aws configure
def get_aws_credentials():
    with open(os.path.expanduser("~/.aws/credentials")) as f:
        for line in f:
            # print(line.strip().split(' = '))
            try:
                key, val = line.strip().split(' = ')
                if key == 'aws_access_key_id':
                    aws_access_key_id = val
                elif key == 'aws_secret_access_key':
                    aws_secret_access_key = val
            except ValueError:
                pass
    return aws_access_key_id, aws_secret_access_key


@task
def extract_from_ya_s3(spark: SparkSession, date: str = None, entity_name: str = None):
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
    # df_spark = spark.createDataFrame(df)
    #
    # partitioned_df = df_spark.repartition(
    #     col("year"), col("country"), col("club"), col("date"), col("entity")
    # )
    #
    # # Сохранить каждый файл в формате parquet локально
    # partitioned_df.write.partitionBy("year", "country", "club", "date", "entity").mode(
    #     "overwrite"
    # ).parquet(S3_PATH_COMMON)
    #
    print(S3_PATH_COMMON)
    local_to_s3(S3_PATH_COMMON)


def local_to_s3(local_dir: str):
    import subprocess
    s3_bucket = 's3://euro-stat/'

    # aws s3 cp --recursive --exclude "*" --include "*.parquet" /path/to/local_dir s3://s3_bucket/s3_prefix/

    command = ['aws', 's3', 'cp', '--recursive', '--exclude', '"*" ', '--include', '"*.parquet"', local_dir,
               s3_bucket + local_dir, "--endpoint-url", "https://storage.yandexcloud.net"]

    access_key, secret_key = get_aws_credentials()
    subprocess.run(f"export AWS_ACCESS_KEY_ID={access_key}")
    subprocess.run(f"export AWS_SECRET_ACCESS_KEY={secret_key}")
    print(' '.join(command))
    # subprocess.run(command, check=True)


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


@flow(name="clubelo pipeline flow")
def data_pipeline(date: str):
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
        extract_from_ya_s3(spark_session, date=date, entity_name='rating')


if __name__ == "__main__":
    data_pipeline('2023-01-01')
