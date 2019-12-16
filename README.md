Usage:
   Setup the PySpark env (need Java 8)

   Run : "source ~/configure_pyspark.sh"

   Confirm :
       echo $JAVA_HOME
       java -version


# To run PySpark on Google Colab (Linux commands need ! as prefix):
* !apt-get install openjdk-8-jdk-headless -qq > /dev/null
* !wget -q https://www-us.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
* !tar xf spark-2.4.4-bin-hadoop2.7.tgz
* !pip install -q findspark

# Setup PySpark env
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.4.4-bin-hadoop2.7"

# Work w/ PySpark
import findspark
findspark.init()

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.build.getOrCreate()

# confirm pyspark working by typing "spark"
