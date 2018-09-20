#!/usr/bin/env bash

echo "Usage : "
echo "        To effectively configure Java 8 for Spark "
echo "        Instead of running as "
echo "             sh configure_pyspark.sh "
echo "             or "
ehco "             ./configure_pyspark.sh "
echo "        Run this shell script as : "
echo "        source ~/.configure_pyspark.sh" 
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
echo "For PySpark configured JAVA_HOME="$JAVA_HOME
echo
export PATH="$JAVA_HOME/bin:$PATH"
echo "For PySpark configured PATH="$PATH

echo
echo "************************************"
echo "* To start PySpark, Type : pyspark *"
echo "************************************"
echo
#pyspark
