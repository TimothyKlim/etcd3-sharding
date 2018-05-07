import akka.actor._
import akka.Done
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl._
import com.coreos.jetcd._
import com.coreos.jetcd.data._
import com.coreos.jetcd.kv._
import com.coreos.jetcd.op._
import com.coreos.jetcd.options._
import com.coreos.jetcd.Watch._
import com.coreos.jetcd.watch._, WatchEvent.EventType
import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.{blocking, Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Main extends App with LazyLogging {
  implicit val sys = ActorSystem()
  implicit val mat = ActorMaterializer()
  import sys.dispatcher

  val leaseTtl = 5
  val leaderKey = "/leader"
  val keySeq = ByteSequence.fromString(leaderKey)
  val nodeId = 1
  val nodeIdKey = nodeId.toString
  val nodeIdSeq = ByteSequence.fromString(nodeIdKey)
  println(s"Watch $leaderKey key\nNode id: $nodeId")

  val client = Client.builder().endpoints("http://127.0.0.1:2379").lazyInitialization(true).build()
  val kvClient = client.getKVClient()
  val leaseClient = client.getLeaseClient()

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

  def revoke(leaseClient: Lease, leaseId: Long): Future[Unit] =
    leaseClient.revoke(leaseId).toScala.map(_ => ())

  def becomeLeader(kvClient: KV, leaseClient: Lease): Future[Option[Long]] =
    for {
      grant <- leaseClient.grant(leaseTtl).toScala
      leaseId = grant.getID()
      opt = PutOption.newBuilder().withLeaseId(leaseId).build()
      keyCmp = new Cmp(keySeq, Cmp.Op.EQUAL, CmpTarget.version(0))
      res <- kvClient.txn().If(keyCmp).Then(Op.put(keySeq, nodeIdSeq, opt)).commit().toScala.transformWith {
        case Success(txnRes) if txnRes.isSucceeded => Future.successful(Some(leaseId))
        case _                                     => revoke(leaseClient, leaseId).map(_ => None)
      }
    } yield res

  val etcdEventsSource = Source
    .unfoldResourceAsync[List[EtcdEvent], Watcher](() => getWatcher(client), electionWatcher, closeWatcher)
    .mapConcat(identity)

  etcdEventsSource.runWith(Sink.foreach(evt => println(s"Etcd event: $evt")))

  val leaseIdRef = new AtomicLong()

  def revokeLease(): Future[Unit] =
    leaseIdRef.get() match {
      case 0L => Future.unit
      case leaseId =>
        revoke(leaseClient, leaseId).andThen {
          case _ => leaseIdRef.set(0L)
        }
    }

  scala.sys.addShutdownHook(Await.result(revokeLease(), Duration.Inf))

  def nodeEvents() = {
    val flow = Source
      .tick(1.second, 1.second, Event.Tick)
      .buffer(size = 1, OverflowStrategy.dropHead)
      .merge(etcdEventsSource.map(Event.Etcd(_)))
      .prepend(Source.fromFuture(revokeLease().map(_ => Event.Init)))
      .scanAsync[NodeState](NodeState.Empty) {
        case (NodeState.Follower, Event.Etcd(EtcdEvent.Delete)) | (NodeState.Empty, Event.Init) =>
          becomeLeader(kvClient, leaseClient).map {
            case Some(leaseId) => NodeState.Confirmation(leaseId)
            case _             => NodeState.Follower
          }
        case (st @ NodeState.Follower, _) => Future.successful(st)
        case (st @ NodeState.Leader(leaseId), Event.Tick) =>
          leaseClient.keepAliveOnce(leaseId).toScala.map(_ => st)
        case (NodeState.Leader(leaseId), _) =>
          revoke(leaseClient, leaseId).map(_ => NodeState.Follower)
        case (NodeState.Confirmation(leaseId), Event.Etcd(EtcdEvent.Put(nodeValue))) =>
          if (nodeValue == nodeIdKey) {
            leaseIdRef.set(leaseId)
            Future.successful(NodeState.Leader(leaseId))
          } else revoke(leaseClient, leaseId).map(_ => NodeState.Follower)
        case (st, Event.Tick) => Future.successful(st)
        case (st, evt) =>
          logger.warn(s"Become a follower from [state=$st] by [evt=$evt]")
          Future.successful(NodeState.Follower)
      }
      .statefulMapConcat[NodeEvent] { () =>
        var _prevEvent = Option.empty[NodeEvent]

        { st: NodeState =>
          val eventOpt = st match {
            case _: NodeState.Leader => Some(NodeEvent.Leader)
            case NodeState.Follower  => Some(NodeEvent.Follower)
            case _                   => None
          }
          eventOpt match {
            case Some(event) if !_prevEvent.contains(event) =>
              _prevEvent = eventOpt
              List(event)
            case _ => Nil
          }
        }
      }
      .watchTermination() { (_, cb) =>
        cb.transformWith { _ =>
          revokeLease()
        }
      }
      .async

    RestartSource
      .withBackoff(minBackoff = 1.second, maxBackoff = 3.seconds, randomFactor = 0.25)(() => flow)
  }

  nodeEvents.runWith(Sink.foreach(st => println(s"state: $st")))

  Await.result(sys.whenTerminated, Duration.Inf)
}

sealed trait NodeState
object NodeState {
  case object Empty extends NodeState
  case object Follower extends NodeState
  final case class Confirmation(leaseId: Long) extends NodeState
  final case class Leader(leaseId: Long) extends NodeState
}

sealed trait NodeEvent
object NodeEvent {
  case object Follower extends NodeEvent
  case object Leader extends NodeEvent
}

sealed trait EtcdEvent
object EtcdEvent {
  case object Delete extends EtcdEvent
  final case class Put(value: String) extends EtcdEvent
}

sealed trait Event
object Event {
  case object Tick extends Event
  case object Init extends Event
  final case class Etcd(evt: EtcdEvent) extends Event
}
