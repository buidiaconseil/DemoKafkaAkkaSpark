# DemoKafkaAkkaSpark
Demo of RSS management and Reporting with Kafka Akka and Spark

# Needs 
- Spark installed on directory spark-2.3.1-bin-hadoop2.7
- SBT installed
- Python with modules : yaml json lxml bs4 kafka
- Kafka started
    see https://hub.docker.com/r/wurstmeister/kafka/

# See the Presentation
https://github.com/buidiaconseil/DemoKafkaAkkaSpark/blob/master/KafkaAkkaDemoPres.pdf

# Generate content.rss
Get the content from the web and push the resulted xml into the file content.rss

    python pullRSS.py

# Push the content of content.rss
We put in json the file content.json

    python pushRSS.py

# Demo en Akka Stream
In the directory `akkastream`

5 demos:
- demo0 : consume the flow and get it on console
- demo1 : consume and get just the words on console
- demo2 : groupBy parallel of demo0
- demo3 : groupby + merge
- demo4 : A complex flow to calulate an iterative TFIDF

to test launch on the directory `akkastream`:

    sdb "run demo0"
    sdb "run demo1"
    sdb "run demo2"
    sdb "run demo3"
    sdb "run demo4"

# Demo on Spark
Spark installed on directory spark-2.3.1-bin-hadoop2.7
From the root directory , launch 

    sparkStarterSup.sh -i kafkaSpark.scala

This first demo launch a consumer and get every 5 seconds the json description

    sparkStarterSup.sh -i kafkaSparkTFIDF.scala
    
This first demo launch a consumer,get every 5 seconds the json description, push it on a memory table and we launch manualy the calculation of the TFIDF

# License
See LICENSE file

 Copyright 2018 [Buisson Diaz Conseil](http://www.buissondiaz.com)
 
 Licensed under the Apache License, Version 2.0 (the "License")






