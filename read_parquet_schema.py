#!/usr/bin/env python

###
### Author : LChawathe
###

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as paq

PARQUET_FIlE_PATH = 'test_data/flight_2020-04-27_part_0.parquet'

table = paq.read_table(source=PARQUET_FIlE_PATH)
pandas_df = table.to_pandas()

schema = pa.Schema.from_pandas(pandas_df)
print('====== Schema (thru PyArrow/Pandas) ======')
print(schema)
print()
print('====== Data ======')
print(pandas_df)

# ---------

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("PySpark_Parquet_schema") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('ERROR')

spark_df = spark.read.parquet(PARQUET_FIlE_PATH)

print('====== Schema (Spark) ======')
spark_df.printSchema()
print()
print('====== Data ======')
spark_df.show(n=10, truncate=False)

spark.stop()