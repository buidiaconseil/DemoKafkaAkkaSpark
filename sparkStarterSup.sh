#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
echo "$BASEDIR"


spark-2.3.1-bin-hadoop2.7/bin/spark-shell --driver-memory 4G --executor-memory 4G --executor-cores 2 --jars $BASEDIR/spark-streaming-kafka-0-10_2.11-2.3.1.jar,$BASEDIR/spark-sql-kafka-0-10_2.11-2.3.1.jar,$BASEDIR/kafka-clients-1.1.0.jar $*
