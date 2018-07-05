#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
echo "$BASEDIR"
BASEDIR=/mnt/d/Dev/DemoKafkaAkkaSpark

#spark-2.3.1-bin-hadoop2.7/bin/spark-shell --conf "spark.driver.extraJavaOptions=-Dhttp.proxyHost=noevipncp2n.edf.fr -Dhttp.proxyPort=3134 -Dhttps.proxyHost=noevipncp2n.edf.fr -Dhttps.proxyPort=3134" --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.1
#spark-2.3.1-bin-hadoop2.7/bin/spark-shell --conf "spark.driver.extraJavaOptions=-Dhttp.proxyHost=si-devops-proxy.edf.fr -Dhttp.proxyPort=3128 -Dhttps.proxyHost=si-devops-proxy.edf.fr -Dhttps.proxyPort=3128" --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.1
spark-2.3.1-bin-hadoop2.7/bin/spark-shell  --driver-class-path $BASEDIR --jars $BASEDIR/spark-streaming-kafka-0-10_2.11-2.3.1.jar,$BASEDIR/spark-sql-kafka-0-10_2.11-2.3.1.jar,$BASEDIR/kafka-clients-1.1.0.jar $1
