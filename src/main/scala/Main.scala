import akka.actor._
import akka.Done
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.coreos.jetcd._
import com.coreos.jetcd.data._
import com.coreos.jetcd.op._
import com.coreos.jetcd.options._
import com.coreos.jetcd.watch._
import com.coreos.jetcd.Watch._
import java.security.SecureRandom
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.{blocking, Await, Future}
import scala.concurrent.duration._
import scala.util.Random

object Main extends App {
  implicit val sys = ActorSystem()
  implicit val mat = ActorMaterializer()
  import sys.dispatcher

  val leaseTtl = 5
  val leaderKey = "/leader"
  val keySeq = ByteSequence.fromString(leaderKey)
  val rand = new Random(new SecureRandom)
  val nodeId = rand.alphanumeric.take(5).mkString
  val nodeIdSeq = ByteSequence.fromString(nodeId)
  println(s"Watch $leaderKey key\nNode id: $nodeId")

  def getWatcher(client: Client): Future[Watcher] =
    Future(blocking(client.getWatchClient().watch(keySeq)))

  def electionWatcher(w: Watcher): Future[Option[List[WatchEvent]]] =
    Future(blocking {
      val xs = w.listen().getEvents().asScala.toList
      Some(xs)
    })

  def closeWatcher(w: Watcher): Future[Done] =
    Future(blocking { w.close(); Done })

  val client = Client.builder().endpoints("http://127.0.0.1:2379").lazyInitialization(true).build()
  val kvClient = client.getKVClient()
  val leaseClient = client.getLeaseClient()

  Source
    .unfoldResourceAsync[List[WatchEvent], Watcher](() => getWatcher(client), electionWatcher, closeWatcher)
    .mapConcat(identity)
    .runWith(Sink.foreach { event =>
      val kv = event.getKeyValue()
      println((event.getEventType(), kv.getKey().toStringUtf8(), kv.getValue().toStringUtf8()))
    })

  val keyCmp = new Cmp(keySeq, Cmp.Op.EQUAL, CmpTarget.version(0))

  for {
    grantRes <- leaseClient.grant(leaseTtl).toScala
    option = PutOption.newBuilder().withLeaseId(grantRes.getID()).build()
    res <- kvClient.txn().If(keyCmp).Then(Op.put(keySeq, nodeIdSeq, option)).commit().toScala
  } yield println(s"tx: $res, success: ${res.isSucceeded}")

  Await.result(sys.whenTerminated, Duration.Inf)
}
