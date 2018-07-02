import akka.stream._
import akka.stream.scaladsl._


import akka.{ NotUsed, Done }
import akka.actor.ActorSystem
import akka.util.ByteString
import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.Paths
import scala.collection.mutable._
import akka.kafka._
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

object Main extends App {
  private def getPipelineSource: Source[String, Future[IOResult]] = {
    FileIO.fromPath(Paths.get("../content.rss"))
      .via(Framing.delimiter(ByteString("\n"), 32000000)
        .map(_.utf8String)
      )
  }
  val mapper = new ObjectMapper();
  val stopWord: HashSet[String] = HashSet()
  val filename = "stopWord.txt"
  for (line <- scala.io.Source.fromFile(filename).getLines) {
    stopWord += line
  }

  println("Hello, World!")
  val  timeout:Timeout = new Timeout(Duration.create(5, "seconds"))
  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()  

 // val source: Source[Int, NotUsed] = Source(1 to 100)
  

  //val done: Future[Done] = source.runForeach(i ⇒ println(i))(materializer)
 

  implicit val ec = system.dispatcher
  //done.onComplete(_ ⇒ system.terminate())
 /*def getOutput: Future[Done] = {
    getPipelineSource
      .map(_.toString)
      .log("Before start")
      .map(_.replaceAll("<[^>]*>", ""))
      .map( line =>
        Source(line.split(" +").toList)
        .map(_.replaceAll("\\p{C}|\\s+|\\r$|\\\\t|\\\\n|\\\\r", "").trim)
        .filterNot(stopWord.contains(_))
        .filterNot(_.contains("http"))
        .filterNot(_.isEmpty).runWith(Sink.seq)
      )
      .runForeach(x => print(x))(materializer)
  }*/
  def terminateWhenDone(result: Future[Done]): Unit =
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
}
  
  var configString:String="kafka-clients.enable.auto.commit=false\npoll-interval=50ms\npoll-timeout=50ms\nstop-timeout=30s\nclose-timeout=20s\ncommit-timeout=15s\ncommit-time-warning=1s"
  configString = configString+"\nwakeup-timeout = 3s\nmax-wakeups = 10\ncommit-refresh-interval = infinite\nwakeup-debug = true\nuse-dispatcher=akka.kafka.default-dispatcher\nwait-close-partition=500ms"
  val  config:Config = ConfigFactory.parseString(configString)
  val consumerSettings:ConsumerSettings[String, String ] =
    ConsumerSettings.create(config, new StringDeserializer(), new StringDeserializer())
        .withBootstrapServers("kafka:9092")
        .withGroupId("group1")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  
  def business(key: String, value: String): Future[Done] = ???
val sayFlow: Flow[String, String, akka.NotUsed] =
    Flow[String].map { s =>
      s + "."
}
 val control =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics("topic1"))
        
        .mapAsync(10) { msg =>
          business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
        }
        
        .mapAsync(5)(offset => offset.commitScaladsl())
        .toMat(Sink.foreach(println))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
         
    // #atLeastOnce

    terminateWhenDone(control.drainAndShutdown())

  /*val control: Consumer.Control =
        Consumer
            .committableSource(consumerSettings, Subscriptions.topics("rss-flow"))
            .map(msg =>  msg.record.value)
            .map( mapper.readValue[Map[String, String]](_, new TypeReference[Map[String, String]](){}))
            .map( line => line.get("description").getOrElse (""))
            .map( line =>
              Source(line.split(" +").toList)
              .map(_.replaceAll("\\p{C}|\\s+|\\r$|\\\\t|\\\\n|\\\\r", "").trim)
              .filterNot(stopWord.contains(_))
              .filterNot(_.contains("http"))
              .filterNot(_.isEmpty).runWith(Sink.seq)
            )
            .map( seqi => Await.result(seqi,Duration("10 sec")))
            
            .to(Sink.foreach(it => System.out.println("Done with " + it)))
            .run()
            .drainAndShutdown()*/
  //getOutput.onComplete(_ ⇒ system.terminate())
}