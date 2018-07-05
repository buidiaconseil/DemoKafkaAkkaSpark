import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime


spark.conf.set("spark.sql.shuffle.partitions", 2)
spark.conf.set("spark.default.parallelism", 10)

// http://163.113.136.75:4040
// Setup connection to Kafka
val kafka = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "rss-flow").option("startingOffsets", "earliest").load()

// split lines by whitespace and explode the array as rows of `word`
var df = kafka.withWatermark("timestamp", "5 seconds").select($"timestamp",explode(split(get_json_object(($"value").cast("string"), "$.description"), "\\s+")).as("word"))
df = df.groupBy($"word",window($"timestamp", "5 seconds")).count


val query = df.writeStream.outputMode("append").format("console").trigger(ProcessingTime("5 seconds")).start()
query.awaitTermination()

// follow the word counts as it updates
display(df.select($"word", $"count"))
