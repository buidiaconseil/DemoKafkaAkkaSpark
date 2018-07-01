import akka.stream._
import akka.stream.scaladsl._


import akka.{ NotUsed, Done }
import akka.actor.ActorSystem
import akka.util.ByteString
import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.Paths
import scala.collection.mutable._


object Main extends App {
  private def getPipelineSource: Source[String, Future[IOResult]] = {
    FileIO.fromPath(Paths.get("../content.rss"))
      .via(Framing.delimiter(ByteString("\n"), 32000000)
        .map(_.utf8String)
      )
  }
  val stopWord: HashSet[String] = HashSet()
  val filename = "stopWord.txt"
  for (line <- scala.io.Source.fromFile(filename).getLines) {
    stopWord += line
  }

  println("Hello, World!")
  
  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()  

  val source: Source[Int, NotUsed] = Source(1 to 100)
  

  val done: Future[Done] = source.runForeach(i ⇒ println(i))(materializer)
 

  implicit val ec = system.dispatcher
  //done.onComplete(_ ⇒ system.terminate())
 def getOutput: Future[Done] = {
    getPipelineSource
      .map(_.toString)
      .log("Before CSV Handler")
      .map(_.replaceAll("<[^>]*>", ""))
      .map( line =>
        Source(line.split(" +").toList)
        .map(_.replaceAll("\\p{C}|\\s+|\\r$|\\\\t|\\\\n|\\\\r", "").trim)
        .filterNot(stopWord.contains(_))
        .filterNot(_.contains("http"))
        .filterNot(_.isEmpty).runWith(Sink.seq)
      )
      .runForeach(x => println(x))(materializer)
  }
  getOutput.onComplete(_ ⇒ system.terminate())

}