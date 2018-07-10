import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.linalg.Vectors

spark.conf.set("spark.sql.shuffle.partitions", 2)
spark.conf.set("spark.default.parallelism", 10)

// http://163.113.136.75:4040
// Setup connection to Kafka
val kafka = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "rss-flow").option("startingOffsets", "earliest").load()

// split lines by whitespace and explode the array as rows of `word`
var df = kafka.withWatermark("timestamp", "5 seconds").select($"timestamp",explode(split(get_json_object(($"value").cast("string"), "$.description"), "\\s+")).as("word"))
df = df.groupBy($"word",window($"timestamp", "5 seconds")).count


val query = df.writeStream.outputMode("append").format("console").trigger(ProcessingTime("5 seconds")).start()


