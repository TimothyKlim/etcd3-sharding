import akka.Done
import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.coreos.jetcd._
import com.coreos.jetcd.data._
import com.coreos.jetcd.options._
import com.coreos.jetcd.Watch._
import com.coreos.jetcd.watch._
import scala.collection.JavaConverters._
import scala.concurrent.{Future, blocking, Await}
import scala.concurrent.duration._
import java.security.SecureRandom
import scala.util.Random

object Main extends App {
  implicit val sys = ActorSystem()
  implicit val mat = ActorMaterializer()
  import sys.dispatcher

  val rand = new Random(new SecureRandom)
  val nodeId = rand.alphanumeric.take(5).mkString
  println(s"node id: $nodeId")

  val client = Client.builder().endpoints("http://127.0.0.1:2379").build()
  Source
    .unfoldResourceAsync[List[WatchEvent], Watcher](
      () => Future(blocking(client.getWatchClient().watch(ByteSequence.fromString("/leader")))),
      { watcher => Future(blocking(Some(watcher.listen().getEvents().asScala.toList))) },
      { watcher => Future(blocking { watcher.close(); Done })} )
    .mapConcat(identity)
    .runWith(Sink.foreach { event =>
      val kv = event.getKeyValue()
      println(
        (event.getEventType(),
         kv.getKey().toStringUtf8(),
         kv.getValue().toStringUtf8()))
    })

  Await.result(sys.whenTerminated, Duration.Inf)
}
