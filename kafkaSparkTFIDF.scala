import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.{CountVectorizer, Tokenizer}
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
val kafka2 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "rss-flow").option("startingOffsets", "earliest").load()

// split lines by whitespace and explode the array as rows of `word`
var df2 = kafka2.withWatermark("timestamp", "5 seconds").select($"timestamp",get_json_object(($"value").cast("string"), "$.title").as("description"))
df2 = df2.groupBy(window($"timestamp", "5 seconds"),$"description").count()
val query2 = df2.writeStream.queryName("description").outputMode("complete").format("memory").start()
spark.sql("select * from description").count
Thread.sleep(6000)

val descript=spark.sql("select description from description").distinct
descript.show()

val tokenizer = new Tokenizer().setInputCol("description").setOutputCol("words")
val wordsData = tokenizer.transform(descript.na.drop(Array("description")))

val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("rawFeatures").setNumFeatures(20)

val featurizedData = hashingTF.transform(wordsData)
// alternatively, CountVectorizer can also be used to get term frequency vectors

val idf = new IDF().setInputCol(hashingTF.getOutputCol).setOutputCol("features")
val idfModel = idf.fit(featurizedData)

val rescaledData = idfModel.transform(featurizedData)
rescaledData.select("description", "words","features").show()
rescaledData.head

