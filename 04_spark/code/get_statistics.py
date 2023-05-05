import sys
import os
from random import random
from operator import add
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


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


access_key, secret_key = get_aws_credentials()

spark = SparkSession \
    .builder \
    .appName("Pyspark S3 reader") \
    .getOrCreate()

sc = spark.sparkContext

# remove this block if use core-site.xml and env variable
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://storage.yandexcloud.net")
sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", secret_key)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secret_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")

df = spark.read.parquet('s3a://euro-stat/sports.ru/football/*/*/*/*/tournament_calendar.parquet')
