#!/usr/bin/env bash

echo "Usage : "
echo "        To effectively configure Java 8 for Spark "
echo "        Instead of running as "
echo "             sh configure_pyspark.sh "
echo "             or "
echo "             ./configure_pyspark.sh "
echo "        Run this shell script as : "
echo "        source ~/.configure_pyspark.sh" to properly export variables
echo

echo ">>>>>>>>> Configuring PySpark with Python 3.7 ........."
echo $(which python3.7)
echo
export PYSPARK_PYTHON=python3.7
export PYSPARK_DRIVER_PYTHON=python3.7
echo "For PySpark configured $PYSPARK_PYTHON"
echo

echo ">>>>>>>>> Configuring PySpark with Java 8 ........."
echo
echo "Current JAVA_HOME="$JAVA_HOME
curr_java_exec_path=$(readlink -f $(which java))
echo "Current Java="$curr_java_exec_path
echo
echo "Current PATH="$PATH
echo

# Extract java-8 base dir
java_8_exec_path=$(update-alternatives --list java | grep java-8)
JAVA_8_HOME=$(dirname $(dirname $java_8_exec_path))

# Set JAVA_HOME and PATH to point to Java 8
export JAVA_HOME=$JAVA_8_HOME
echo
echo "For PySpark configured JAVA_HOME=$JAVA_HOME"
echo

# Set SPARK_HOME to point to Spark installation from Opt folder
export SPARK_HOME="/home/lalitstar/opt/spark-3.0.0-preview-bin-hadoop2.7"
echo
echo "For PySpark configured SPARK_HOME=$SPARK_HOME"
echo

# Configure PATH
export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH"
echo
echo "For PySpark configured PATH=$PATH"
echo

### Configure Jupyter Notebook (First ensure to run: sudo pip3 install ipython)
### export PYSPARK_SUBMIT_ARGS="pyspark-shell"
### export PYSPARK_DRIVER_PYTHON=ipython
### export PYSPARK_DRIVER_PYTHON_OPTS='notebook' pyspark
### echo
### echo "For PySpark configured Jupyter Notebook"
### echo

echo "************************************"
echo "* To start PySpark, Type : pyspark *"
echo "************************************"
echo
#pyspark
