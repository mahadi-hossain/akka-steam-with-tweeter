//#full-example
package com.example

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.SpawnProtocol
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import akka.actor.Status
import scala.util.Failure
import scala.util.Success
import akka.actor.typed.Dispatchers
import scala.concurrent.ExecutionContext
import akka.actor.typed.Props
import scala.concurrent.Future
import akka.stream._
import akka.stream.scaladsl._
import akka.NotUsed
import akka.util.ByteString
import java.nio.file.Paths
import akka.actor
import akka.Done
import scala.concurrent.duration._
import scala.util.Random
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.ContentType
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.HttpCharsets
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object Main extends App {

  def apply(): Behavior[SpawnProtocol.Command] = SpawnProtocol.apply()

  implicit val actorSystem = ActorSystem(Main(), "system")

  implicit val ex = actorSystem.classicSystem.dispatcher

  val request = HttpRequest.apply()

  val conf = ConfigFactory.load()

  val accessToken = conf.getString("tweeter.access-token")
  val url = conf.getString("tweeter.streamUrl")

  val source = RestartSource.withBackoff(
    RestartSettings.apply(
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2
    )
  ) { () =>
    val response: Future[HttpResponse] =
      Http().singleRequest(createHttpRequest(s"Bearer $accessToken", Uri(url)))

    Source.futureSource {
      response.failed.foreach(t =>
        System.err.println(s"Request has been failed with $t")
      )

      response.map(resp => {
        resp.status match {
          case OK => resp.entity.withoutSizeLimit().dataBytes
          case code =>
            val text: Source[String, Any] =
              resp.entity.dataBytes.map(_.utf8String)
            val error = s"Unexpected status code: $code, $text"
            System.err.println(error)
            Source.failed(new RuntimeException(error))
        }
      })
    }
  }

  val tweets = source.map(_.utf8String)

  val printSink = Sink.foreach[String](println)
  val countSink = Sink.fold[Int, String](0) { (ag, _) =>
    println(ag + 1)
    ag + 1
  }

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._

    val bcast: UniformFanOutShape[String,String] = b.add(Broadcast[String](2))
    tweets ~> bcast.in
    bcast.out(0) ~>  printSink
    bcast.out(1) ~> countSink
    ClosedShape
  })


  g.run()

  private def createHttpRequest(header: String, source: Uri): HttpRequest = {
    // creating required HTTP headers to perform OAuth authorizaton
    val httpHeaders = List(
      HttpHeader.parse("Authorization", header) match {
        case HttpHeader.ParsingResult.Ok(h, _) => Some(h)
        case _                                 => None
      },
      HttpHeader.parse("Accept", "*/*") match {
        case HttpHeader.ParsingResult.Ok(h, _) => Some(h)
        case _                                 => None
      }
    ).flatten

    HttpRequest(
      method = HttpMethods.GET,
      uri = source,
      headers = httpHeaders
    )
  }









}
