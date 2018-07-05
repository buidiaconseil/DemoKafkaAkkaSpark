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



// http://163.113.136.75:4040
// Setup connection to Kafka
val kafka2 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "rss-flow").option("startingOffsets", "earliest").load()

// split lines by whitespace and explode the array as rows of `word`
var df2 = kafka2.withWatermark("timestamp", "5 seconds").select($"timestamp",get_json_object(($"value").cast("string"), "$.title").as("description"))
df2 = df2.groupBy($"description",window($"timestamp", "5 seconds")).count()
val query2 = df2.writeStream.queryName("description").outputMode("complete").format("memory").start()


val descript=spark.sql("select * from description")
val tokenizer = new Tokenizer().setInputCol("description").setOutputCol("words")
val wordsData = tokenizer.transform(descript.na.fill(Map("description" -> "")))
val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

val featurizedData = hashingTF.transform(wordsData)
// alternatively, CountVectorizer can also be used to get term frequency vectors

val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
val idfModel = idf.fit(featurizedData)

val rescaledData = idfModel.transform(featurizedData)
rescaledData.select("description", "features").show()



//val numClusters = 2
//val numIterations = 20
//val vectors = allDF.rdd.map(r => Vectors.dense( r.getDouble(5), r.getDouble(6), r.getDouble(7), r.getDouble(8), r.getDouble(9) ))
//val clusters = KMeans.train(rescaledData.select( "features").rdd.map(r=>Vectors.dense(r.getAs[Vector[Double]]("features").toArray)), numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
//val WSSSE = clusters.computeCost(rescaledData)

//query.awaitTermination()
//query2.awaitTermination()
// follow the word counts as it updates
//display(df.select($"word", $"count"))
