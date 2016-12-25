package reactive

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._

import scala.concurrent.Future


import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by gosha on 12/25/16.
  */
object Main extends App {
  final case class Author(handle: String)

  final case class Hashtag(name: String)

  final case class Tweet(author: Author, timestamp: Long, body: String) {
    def hashtags: Set[Hashtag] =
      body.split(" ").collect { case t if t.startsWith("#") => Hashtag(t) }.toSet
  }

  val akkaTag = Hashtag("#akka")

  implicit val system = ActorSystem("reactive-tweets")
  implicit val materializer = ActorMaterializer()



  val tweets = Source(List(
    Tweet(Author("John"), 1000, "#TAG2 #TAG3 #TAG4 #TAG5"),
    Tweet(Author("James"), 2000, "#TAG6 #akka #TAG7 #TAG8"),
    Tweet(Author("Bob"), 3000, "#Body bodybody #TAG12"),
    Tweet(Author("Tom"), 4000, "#TAG9 #akka #TAG10 #TAG811")))

  val count: Flow[Tweet, Int, NotUsed] = Flow[Tweet].map(_ => 1)

  val sumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _)

  val counterGraph: RunnableGraph[Future[Int]] =
    tweets
      .via(count)
      .toMat(sumSink)(Keep.right)

  val sum: Future[Int] = counterGraph.run()

  sum.foreach(c => println(s"Total tweets processed: $c"))



//  val authors: Source[Author, NotUsed] =
//    tweets
//      .filter(_.hashtags.contains(akkaTag))
//      .map(_.author)
//
//  authors.runWith(Sink.foreach(println))




  val writeAuthors = Sink.foreach[Author](println)
  val writeHashtags = Sink.foreach[Hashtag](println)

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._

    val bcast = b.add(Broadcast[Tweet](2))
    tweets ~> bcast.in
    bcast.out(0) ~> Flow[Tweet].map(_.author) ~> writeAuthors
    bcast.out(1) ~> Flow[Tweet].mapConcat(_.hashtags.toList) ~> writeHashtags
    ClosedShape
  })
  g.run()
}