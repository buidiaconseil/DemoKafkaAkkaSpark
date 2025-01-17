import akka.stream._
import akka.stream.scaladsl._
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import java.util.Arrays
import akka.{NotUsed, Done}
import akka.actor.ActorSystem
import akka.util.ByteString
import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.Paths
import scala.collection.mutable._
import akka.kafka._
import scala.math._
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Producer
import java.util.concurrent.CompletableFuture
import com.typesafe.config._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, JsonScalaEnumeration}
import akka.japi.Pair
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.util.Timeout
import scala.util.{Failure, Success}
import GraphDSL.Implicits._

//https://www.linkedin.com/pulse/databricks-apache-spark-streaming-twitter4j-machine-ml-weichberger/
//https://docs.databricks.com/spark/latest/mllib/mllib-pipelines-and-stuctured-streaming.html
object Main extends App {

  var demoStr = "demo0"
  if (args.length == 0) {
    println("dude, i prefer at least one parameter")
  }
  else {
    demoStr = args(0)
  }

  val mapper = new ObjectMapper();
  mapper.registerModule(DefaultScalaModule)
  val stopWord: HashSet[String] = HashSet()
  type ScanResult = (Seq[String], Option[Seq[String]])
  val filename = "stopword.txt"
  for (line <- scala.io.Source.fromFile(filename).getLines) {
    stopWord += line
  }

  val timeout: Timeout = new Timeout(Duration.create(5, "seconds"))
  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()

  implicit val ec = system.dispatcher


  var configString: String = "kafka-clients.enable.auto.commit=false\npoll-interval=50ms\npoll-timeout=50ms\nstop-timeout=30s\nclose-timeout=20s\ncommit-timeout=15s\ncommit-time-warning=1s"
  configString = configString + "\nwakeup-timeout = 3s\nmax-wakeups = 10\ncommit-refresh-interval = infinite\nwakeup-debug = true\nuse-dispatcher=akka.kafka.default-dispatcher\nwait-close-partition=500ms"
  val config: Config = ConfigFactory.parseString(configString)
  val consumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings.create(config, new StringDeserializer(), new StringDeserializer())
      .withBootstrapServers("kafka:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  


  if (demoStr == "demo0") {

    startDemo0
  }
  if (demoStr == "demo1") {
    startDemo1
  }
  if (demoStr == "demo2") {
    startDemo2
  }
  if (demoStr == "demo3") {
    startDemo3
  }
  if (demoStr == "demo4") {
    startDemo4
  }

  private def startDemo0 = {
    val consumerSettings: ConsumerSettings[String, String] =
      ConsumerSettings.create(config, new StringDeserializer(), new StringDeserializer())
        .withBootstrapServers("kafka:9092")
        .withGroupId("group0")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val source = Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("rss-flow"))
      .log("Before start")
      .map(rec => transformToWords(rec.value()))
      .runForeach(x => println(x))(materializer)
  }

  private def startDemo1 = {
    val consumerSettings: ConsumerSettings[String, String] =
      ConsumerSettings.create(config, new StringDeserializer(), new StringDeserializer())
        .withBootstrapServers("kafka:9092")
        .withGroupId("group1")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val source = Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("rss-flow"))
      .log("Before start")
      .map(rec => transformToWords(rec.value()))
      .flatMapConcat(i ⇒ Source(i))
      .runForeach(x => println(x))(materializer)
  }

  /* Group  */
  private def startDemo2 = {
    val consumerSettings: ConsumerSettings[String, String] =
      ConsumerSettings.create(config, new StringDeserializer(), new StringDeserializer())
        .withBootstrapServers("kafka:9092")
        .withGroupId("group2")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("rss-flow"))
      .map(rec => transformToWords(rec.value()))
      .groupBy(2, _.contains("trump"))

      .to(Sink.foreach(x => println(x))).run()
  }

  /*
  // Group and merge  */
  private def startDemo3 = {
    ConsumerSettings.create(config, new StringDeserializer(), new StringDeserializer())
      .withBootstrapServers("kafka:9092")
      .withGroupId("group3")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("rss-flow"))
      .map(rec => transformToWords(rec.value()))
      .groupBy(2, _.contains("trump"))
      .map(a => searchWord(a, "trump"))
      .filterNot(_.isEmpty)
      .mergeSubstreams
      // get a stream of word counts

      .runForeach(x => println(x))(materializer)

  }

  

  private def startDemo4 = {
    ConsumerSettings.create(config, new StringDeserializer(), new StringDeserializer())
      .withBootstrapServers("kafka:9092")
      .withGroupId("group4")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val source = Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("rss-flow"))
      .log("Graph")
      .map(rec => transformToWords(rec.value()))

    val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val in = source
      val starter = Source[RegistryCounter](List[RegistryCounter]((0, collection.mutable.Map[String, Double]())))
      val out = Sink.foreach(println)


      val bcast = builder.add(Broadcast[List[String]](2))
      val merger = builder.add(Merge[RegistryCounter](2))
      val bcastIdf = builder.add(Broadcast[RegistryCounter](2))
      val zipIdf = builder.add(Zip[RegistryCounter, RegistryCounter]())

      val zipIdfTf = builder.add(Zip[Map[String, Double], RegistryCounter]())


      val tf = Flow[List[String]].map(tfFun(_)).log("tf")

      val idfprepare = Flow[List[String]].map(idfstart(_))

      val idf = Flow[(RegistryCounter, RegistryCounter)].map(idfFun(_))

      val mergetfidf = Flow[(Map[String, Double], RegistryCounter)].map(mergetfidfFun(_))


      in ~> bcast ~> tf ~> zipIdfTf.in0
      bcast ~> idfprepare ~> zipIdf.in0
      zipIdf.out ~> idf ~> bcastIdf ~> zipIdfTf.in1
      merger <~ bcastIdf
      starter ~> merger
      merger.out ~> zipIdf.in1

      zipIdfTf.out ~> mergetfidf ~> out
      /*in ~> bcast ~>                  tf ~>                         zipIdfTf.in0
            bcast ~> idfprepare ~> zipIdf.in0
                                   zipIdf.out ~> idf ~> bcastIdf ~> zipIdfTf.in1
                                                                    zipIdfTf.out ~>  mergetfidf  ~> out
                                   zipIdf.in1 <~        bcastIdf

      */
      ClosedShape
    }).run
  }


  def business(key: String, value: String): Future[Done] = ???

  // def atMostOnceSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[ConsumerRecord[K, V], Control]
  // committableSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[CommittableMessage[K, V], Control]
  def transformToWords(result: String): List[String] = {
    var retVal: List[String] = null
    try {
      val retMap: Map[String, String] = mapper.readValue[Map[String, String]](result, classOf[Map[String, String]])
      var desc: String = retMap.get("description").getOrElse(" ") + " " + retMap.get("title").getOrElse(" ")
      desc = desc.replaceAll("<[^>]*>", " ").toLowerCase
      desc = desc.replaceAll("\\?|:|,|\\(|\\)|#|\\.|\"|'|\\p{C}|\\s+|\\r$|\\\\t|\\\\n|\\\\r", " ")
      desc = desc.replaceAll("[\\W]|_", " ")
      val streamed: Stream[String] = desc.split(" +").toStream
      retVal = streamed
        .map(mes => mes.trim)
        .filter(mes => !stopWord.contains(mes))
        .filter(mes => !mes.contains("http"))
        .filter(mes => !mes.isEmpty)
        .filter(mes => mes.length > 3).toList
    }
    catch {
      case foo: Exception => foo.printStackTrace
      case _: Throwable => println("Got some other kind of exception")
    }
    return retVal
  }

  def duplicates(prev: ScanResult, next: String): ScanResult = {
    val (acc, result) = prev
    acc match {
      case l :+ last if last.equals(next) => (acc :+ next, None)
      case l :+ last => (Seq(next), if (acc.size > 1) Some(acc) else None)
    }
  }

  def searchWord(result: List[String], word: String): List[String] = {
    if (result.contains(word)) {
      return result
    }
    return List.empty
  }

  def tfFun(words: List[String]): Map[String, Double] = {
    val cache = collection.mutable.Map[String, Double]()
    for (word <- words) {
      var nb = 0.0
      for (wordCount <- words) {
        if (word == wordCount) {
          nb = nb + 1.0
        }
      }
      cache.put(word, nb / words.size.toDouble)
    }

    return cache
  }

  type RegistryCounter = (Double, Map[String, Double])


  def idfFun(registry: (RegistryCounter, RegistryCounter)): RegistryCounter = {
    var nbCol = registry._1._1 + registry._2._1
    val cache = collection.mutable.Map[String, Double]()

    for ((k, v) <- registry._2._2) {
      cache.put(k, v)
    }
    for ((k, v) <- registry._1._2) {
      var sum = v
      if (cache.contains(k)) {
        sum = sum + cache(k)
      }
      cache.put(k, sum)
    }

    return (nbCol, cache)
  }

  def idfstart(words: List[String]): RegistryCounter = {
    val cache = collection.mutable.Map[String, Double]()
    for (word <- words) {

      cache.put(word, 1)
    }

    return (1, cache)
  }

  def mergetfidfFun(words: (Map[String, Double], RegistryCounter)): Map[String, Double] = {
    val cache = collection.mutable.Map[String, Double]()
    var nbDoc = words._2._1
    for ((k, v) <- words._1) {
      if (words._2._2.contains(k)) {
        cache.put(k, v * log10(nbDoc / words._2._2(k)))
      }
    }
    return cache
  }

  def terminateWhenDone(result: Future[Done]): Unit =
  result.onComplete {
    case Failure(e) =>
      system.log.error(e, e.getMessage)
      system.terminate()
    case Success(_) => system.terminate()
  }

  private def getPipelineSource: Source[String, Future[IOResult]] = {
    FileIO.fromPath(Paths.get("../content.rss"))
      .via(Framing.delimiter(ByteString("\n"), 32000000)
        .map(_.utf8String)
      )
  }

}