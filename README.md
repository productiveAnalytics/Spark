Usage:
   Setup the PySpark env (need Java 8)

   Run : "source ~/configure_pyspark.sh"

   Confirm :
       echo $JAVA_HOME
       java -version


# To activate virtual python env
pipenv shell

# To run PySpark on Google Colab

##  Necessary Linux commands (note: need ! as prefix):
* !apt-get install openjdk-8-jdk-headless -qq > /dev/null
* !wget -q https://www-us.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
* !tar xf spark-2.4.4-bin-hadoop2.7.tgz
* !pip install -q findspark

## Setup PySpark env
import os
*  os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
*  os.environ["SPARK_HOME"] = "/content/spark-2.4.4-bin-hadoop2.7"

## Work w/ PySpark
import findspark
findspark.init()

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.build.getOrCreate()

## confirm pyspark working by typing "spark"

---

## Spark-Submit (for Java)
spark-submit --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -verbose:gc -XX:+PrintGCTimeStamps -XX:InitiatingHeapOccupancyPercent=35 -Dlinear.properties.file=./proposal/proposal-conformance-LTS-dev.properties -DexecutionDate=2020/09/16" --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:InitiatingHeapOccupancyPercent=35 -Dlinear.properties.file=./proposal/proposal-conformance-LTS-dev.properties -DexecutionDate=2020/09/16" --name linear-proposalheader-conformed-dev --conf spark.driver.memory=12g --conf spark.driver.cores=2 --conf spark.executor.memory=25g --conf spark.executor.cores=3 --conf spark.kryo.unsafe=true --conf spark.kryoserializer.buffer=300M --conf spark.kryoserializer.buffer.max=1024M --conf spark.task.maxFailures=10 --conf spark.yarn.executor.memoryOverhead=5120m --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=75 --conf spark.executor.extraClassPath=/usr/lib/spark/jars/ --master yarn --deploy-mode cluster --class com.dtci.linear.core.spark.SparkApplication s3://dp-repository-dev/dp-linear-conformance/dp-linear-conformation-Airflow-Oct15__Sept15_as_DayOne.jar

---

Refer: Spark_on_Kubernetes.ppt for Spark on EKS
