import akka.actor._
import akka.Done
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl._
import com.coreos.jetcd._
import com.coreos.jetcd.data._
import com.coreos.jetcd.op._
import com.coreos.jetcd.options._
import com.coreos.jetcd.watch._, WatchEvent.EventType
import com.coreos.jetcd.Watch._
import com.coreos.jetcd.kv._
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

  def electionWatcher(w: Watcher): Future[Option[List[EtcdEvent]]] =
    Future(blocking {
      val xs = w
        .listen()
        .getEvents()
        .asScala
        .toList
        .map { evt =>
          val kv = evt.getKeyValue()
          if (kv.getKey().toStringUtf8() != leaderKey) None
          else
            evt.getEventType() match {
              case EventType.PUT          => Some(EtcdEvent.Put(kv.getValue().toStringUtf8()))
              case EventType.DELETE       => Some(EtcdEvent.Delete)
              case EventType.UNRECOGNIZED => None
            }
        }
        .flatten
      Some(xs)
    })

  def closeWatcher(w: Watcher): Future[Done] =
    Future(blocking { w.close(); Done })

  val client = Client.builder().endpoints("http://127.0.0.1:2379").lazyInitialization(true).build()
  val kvClient = client.getKVClient()
  val leaseClient = client.getLeaseClient()

  val etcdEventsSource = Source
    .unfoldResourceAsync[List[EtcdEvent], Watcher](() => getWatcher(client), electionWatcher, closeWatcher)
    .mapConcat(identity)

  etcdEventsSource.runWith(Sink.foreach(println))

  val eventsSource: Source[Event, _] = etcdEventsSource
  // .collect {
  //   case evt @ EtcdEvent.Delete => evt
  // }
    .map(Event.Etcd(_))
    .prepend(Source.single[Event](Event.Init))

  Source
    .fromFuture(leaseClient.grant(leaseTtl).toScala)
    .flatMapConcat { grant =>
      val opt = PutOption.newBuilder().withLeaseId(grant.getID()).build()
      val keyCmp = new Cmp(keySeq, Cmp.Op.EQUAL, CmpTarget.version(0))
      Source
        .tick(0.seconds, 1.second, Event.Tick)
        .buffer(size = 1, OverflowStrategy.dropHead)
        .merge(eventsSource)
        .scanAsync(Option.empty[TxnResponse]) {
          case (prevTxnOpt, evt: Event) =>
            println("Trying to become a leader")
            kvClient.txn().If(keyCmp).Then(Op.put(keySeq, nodeIdSeq, opt)).commit().toScala.map(Some(_))
        }
    }
    .collect { case Some(res) => res }
    .runWith(Sink.foreach(res => println(s"tx: $res, success: ${res.isSucceeded}")))

  Await.result(sys.whenTerminated, Duration.Inf)
}

sealed trait EtcdEvent
object EtcdEvent {
  case object Delete extends EtcdEvent
  final case class Put(value: String) extends EtcdEvent
}

sealed trait Event
object Event {
  case object Init extends Event
  case object Tick extends Event
  final case class Etcd(evt: EtcdEvent) extends Event
}
