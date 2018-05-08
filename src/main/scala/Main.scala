import akka.actor._
import akka.Done
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl._
import com.coreos.jetcd._
import com.coreos.jetcd.data._
import com.coreos.jetcd.kv.TxnResponse
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
import scala.util.{Success, Try}
import upickle.default._

object Main extends App with LazyLogging {
  implicit val sys = ActorSystem()
  implicit val mat = ActorMaterializer()
  import sys.dispatcher

  // global settings
  val shards = 256
  val namespace = "/ring"

  // node settings
  val leaseTtl = 5L
  val leaderKey = s"$namespace/leader"
  val keySeq = ByteSequence.fromString(leaderKey)
  val nodeId = 1
  val nodeIdKey = nodeId.toString
  val nodeIdSeq = ByteSequence.fromString(nodeIdKey)
  println(s"Watch $leaderKey key\nNode id: $nodeId")

  val client = Client.builder().endpoints("http://127.0.0.1:2379").lazyInitialization(true).build()
  val kvClient = client.getKVClient()
  val leaseClient = client.getLeaseClient()

  def getWatcher(key: ByteSequence, client: Client): Future[Watcher] =
    Future(blocking(client.getWatchClient().watch(key)))

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
    .unfoldResourceAsync[List[EtcdEvent], Watcher](() => getWatcher(keySeq, client), electionWatcher, closeWatcher)
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
          (st match {
            case _: NodeState.Leader => Some(NodeEvent.Leader)
            case NodeState.Follower  => Some(NodeEvent.Follower)
            case _                   => None
          }) match {
            case eventOpt @ Some(event) if !_prevEvent.contains(event) =>
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

  val nodesKeyPrefix = s"$namespace/nodes/"
  val nodesKeySeq = ByteSequence.fromString(nodesKeyPrefix)

  val nodesSource = Source
    .tick(0.seconds, 1.second, ())
    .buffer(size = 1, OverflowStrategy.dropHead)
    .mapAsync(1) { _ =>
      kvClient
        .get(nodesKeySeq, GetOption.newBuilder().withPrefix(nodesKeySeq).build())
        .toScala
        .map(_.getKvs().asScala.toList.map { kv =>
          Try(kv.getKey().toStringUtf8().drop(nodesKeyPrefix.length).toInt) match { // TODO: check key format
            case Success(id) =>
              // println(s"node get by prefix: ${kv.getKey().toStringUtf8() -> kv.getValue().toStringUtf8()}")
              val sharding = read[NodeSharding](kv.getValue().toStringUtf8())
              Some((id, (sharding, kv.getVersion())))
            case _ => None
          }
        }.flatten)
    }
    .async
    .mapAsync(1) { nodes =>
      if (nodes.nonEmpty) {
        val nodesCount: Int = nodes.map(_._1).max
        val nodesMap = nodes.toMap
        val rangeLength = shards / nodesCount.toDouble
        val ranges: Seq[Seq[Int]] = {
          val xs = Range.inclusive(1, shards).grouped(rangeLength.toInt).toIndexedSeq
          if (rangeLength == rangeLength.toInt) xs
          else xs.dropRight(2) ++ Seq(xs.takeRight(2).flatten) // merge last chunk into single
        }
        val emptyBuf = List.empty[(Int, NodeSharding, Long)]
        val (fullShards, intersectShards) =
          Range.inclusive(1, nodesCount).foldLeft((emptyBuf, emptyBuf)) {
            case ((xs, ts), id) =>
              val nodeId = id
              val (sharding, version) = nodesMap.get(id).getOrElse((NodeSharding.empty, 0L))
              val newRange = ranges(id - 1)

              val xss =
                if (sharding.range.sameElements(newRange) || sharding.newRange.exists(_.sameElements(newRange))) xs
                else (nodeId, sharding.copy(newRange = Some(newRange)), version) :: xs

              val intersect = sharding.range.intersect(newRange)
              val tss =
                if (intersect.isEmpty) ts
                else (nodeId, sharding.copy(newRange = Some(intersect)), version) :: ts

              (xss, tss)
          }
        // if all nodes has only intersect shards as active then sync shards or else sync all with intersect shards only
        val nodesShards = if (intersectShards.nonEmpty) intersectShards else fullShards
        if (nodesShards.isEmpty) Future.unit
        else {
          logger.info(s"Apply shards: $nodesShards")
          Future.traverse(nodesShards) {
            case (nodeId, shard, version) =>
              val cmp = new Cmp(nodeKeySeq, Cmp.Op.EQUAL, CmpTarget.version(version))
              kvClient
                .txn()
                .If(cmp)
                .Then(Op.put(ByteSequence.fromString(s"$nodesKeyPrefix$nodeId"),
                             ByteSequence.fromString(write(shard)),
                             PutOption.DEFAULT))
                .commit()
                .toScala
                .map(_.isSucceeded)
          }
        }
      } else Future.unit
    }

  nodesSource
    .runWith(Sink.foreach(node => println(s"nodes event: $node")))
    .onComplete(cb => println(s"nodes source cb: $cb"))

  val nodeKeySeq = ByteSequence.fromString(s"$nodesKeyPrefix$nodeId")

  def nodeWatcher(w: Watcher): Future[Option[List[RingNodeEvent]]] =
    Future(blocking {
      val xs = w
        .listen()
        .getEvents()
        .asScala
        .toList
        .map { evt =>
          val kv = evt.getKeyValue()
          if (nodeKeySeq != kv.getKey()) None
          else
            evt.getEventType() match { // TODO: check key format
              case EventType.PUT =>
                val sharding = read[NodeSharding](kv.getValue().toStringUtf8())
                Some(RingNodeEvent.Sharding(sharding))
              case (EventType.DELETE) =>
                Some(RingNodeEvent.Reset)
              case _ => None
            }
        }
        .flatten
      Some(xs)
    })

  def createNodeShard(key: ByteSequence): Future[TxnResponse] = {
    val nodeKeyCmp = new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0))
    kvClient
      .txn()
      .If(nodeKeyCmp)
      .Then(Op.put(key, ByteSequence.fromString(write(NodeSharding.empty)), PutOption.DEFAULT))
      .commit()
      .toScala
  }

  Source
    .unfoldResourceAsync[List[RingNodeEvent], Watcher](() => getWatcher(nodeKeySeq, client), nodeWatcher, closeWatcher)
    .prepend(Source.fromFuture {
      createNodeShard(nodeKeySeq)
        .flatMap { res =>
          if (res.isSucceeded) Future.successful(List(RingNodeEvent.Sharding.empty))
          else
            kvClient
              .get(nodeKeySeq)
              .toScala
              .map(_.getKvs.asScala
                .map(kv => RingNodeEvent.Sharding(read[NodeSharding](kv.getValue().toStringUtf8)))
                .toList)
        }
    })
    .mapConcat(identity)
    .mapAsync(1) {
      case RingNodeEvent.Reset => createNodeShard(nodeKeySeq)
      case RingNodeEvent.Sharding(s @ NodeSharding(range, Some(newRange))) if !range.sameElements(newRange) =>
        val value = ByteSequence.fromString(write(s.copy(range = newRange, newRange = None)))
        kvClient.put(nodeKeySeq, value).toScala
      case _ => Future.unit
    }
    .runWith(Sink.foreach(evt => println(s"current node event: $evt")))

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

final case class NodeSharding(
    range: Seq[Int], // actual node shard range
    newRange: Option[Seq[Int]] // shard range to apply from a leader
)
object NodeSharding {
  val empty = NodeSharding(Seq.empty, None)

  implicit def rw: ReadWriter[NodeSharding] = macroRW
}

sealed trait RingNodeEvent
object RingNodeEvent {
  final case class Sharding(settings: NodeSharding) extends RingNodeEvent
  object Sharding {
    val empty = Sharding(NodeSharding.empty)
  }
  case object Reset extends RingNodeEvent
}
