import requests 
import datetime
import pandas as pd
from io import StringIO
from pyspark.sql import SparkSession
from pyspark import SparkConf

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Pyspark S3 reader") \
    .config('spark.executor.core', 4)\
    .config('spark.executor.instances', 4) \
    .getOrCreate()


date = '2023-05-01'
r = requests.get(f"http://api.clubelo.com/{date}")
data = StringIO(r.text)

df = pd.read_csv(data, sep=",")
df['year'] = datetime.datetime.strptime(date, "%Y-%m-%d").year
df['date'] = datetime.datetime.strptime(date, "%Y-%m-%d").strftime('%Y%m%d')
df = df.rename(columns={"Country": "country", "Club": "club"}, errors="raise")
print(df)