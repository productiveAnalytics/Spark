from pyspark import SparkConf, SparkContext
from pathlib import Path
import os
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

user_home = Path.home()
# Absolute path : /home/lalitstar/Dev/Data/ml-latest/ratings.csv
# OR file_path = user_home / 'Dev/Data/ml-latest/ratings.csv'
datafile_path = user_home.joinpath('Dev/Data/ml-latest/ratings.csv')
file_name = str(datafile_path)

print ('File: ', file_name , ' exists? ', os.path.exists(file_name), ' isFile? ', os.path.isfile(file_name))

lines = sc.textFile(file_name)

# Remove header from CSV
header_row = lines.first()
print("Header:", header_row)
lines = lines.filter(lambda l: l != header_row)

ratings = lines.map(lambda x: x.split(',')[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
