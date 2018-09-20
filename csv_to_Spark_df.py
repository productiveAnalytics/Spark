from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import avg, mean 
from pathlib import Path
import os
import collections

conf = SparkConf().setMaster("local").setAppName("CSV-to-Spark-DF")
sc = SparkContext(conf = conf)

spark = SparkSession.builder.config("spark.sql.crossJoin.enabled","true").getOrCreate()

sqlCtx = SQLContext(sc)

user_home = Path.home()

data_file_path = user_home / 'Dev/Data/ml-latest/ratings.csv'
data_file_name = str(data_file_path)
print(data_file_name)

df = spark.read.csv(data_file_name, header=True)
df.printSchema()

avg_rating = df.select(avg('rating')).collect()
print('Avg rating:', avg_rating)

mean_rating = df.select(mean('rating')).collect()
print('Mean rating:', mean_rating)
