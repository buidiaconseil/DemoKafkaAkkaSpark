import org.apache.spark.sql.functions.{explode, split}

// Setup connection to Kafka
val kafka = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "rss-flow").option("startingOffsets", "earliest").load()

// split lines by whitespace and explode the array as rows of `word`
val df = kafka.withWatermark("timestamp", "5 seconds").select($"timestamp",explode(split($"value".cast("string"), "\\s+")).as("word")).groupBy(window($"timestamp", "10 seconds"),$"word").count

 val query = df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()

// follow the word counts as it updates
display(df.select($"word", $"count"))
