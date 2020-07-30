#!/usr/bin/env bash

cd ${SPARK_HOME}
pwd
echo "Using Spark at ${SPARK_HOME}"

# Master IP
#SPARK_MASTER_HOST=
SPARK_MASTER_HOST=$(hostname -I | awk '{print $1}')
export SPARK_MASTER_HOST=${SPARK_MASTER_HOST}       # SPARK_MASTER_IP is deprecated
echo "SPARK_MASTER_HOST=${SPARK_MASTER_HOST}"

# Run master
SPARK_MASTER_HOST=${SPARK_MASTER_HOST} ./sbin/start-master.sh
# Open localhost:8080 to confirm Master running with UI

# Master port
SPARK_MASTER_PORT=7077

# Allow connection to master
export SPARK_LOCAL_IP=${SPARK_MASTER_HOST}
echo "Spark configured with SPARK_LOCAL_IP=${SPARK_LOCAL_IP}"

# Setup master url
SPARK_MASTER_URL="spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
echo "SPARK_MASTER_URL=${SPARK_MASTER_URL}"

###
### Cluster-mode (local)
###
./bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master ${SPARK_MASTER_URL} \
--deploy-mode cluster \
--supervise \
--driver-memory 4g \
--driver-cores 2 \
--executor-memory 8g \
--executor-cores 8 \
./examples/jars/spark-examples_2.12-3.0.0.jar 1000

###
### Stand-alone mode
###
# ./bin/spark-submit \
# --class org.apache.spark.examples.SparkPi \
# --master local[*] \
# --supervise \
# --conf spark.driver.memory=4g \
# --conf spark.driver.cores=2 \
# --conf spark.executor.memory=8g \
# --conf spark.executor.cores=8 \
# ./examples/jars/spark-examples_2.12-3.0.0.jar 1000

echo 'Spark is functioning well!'