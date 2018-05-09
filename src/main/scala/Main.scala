import akka.actor._
import akka.Done
import akka.kafka._
import akka.kafka.scaladsl._
import akka.stream.{ActorMaterializer, KillSwitches, OverflowStrategy}
import akka.stream.scaladsl._
import cats.syntax.either._
import com.coreos.jetcd._
import com.coreos.jetcd.data._
import com.coreos.jetcd.kv.TxnResponse
import com.coreos.jetcd.op._
import com.coreos.jetcd.options._
import com.coreos.jetcd.Watch._
import com.coreos.jetcd.watch._, WatchEvent.EventType
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import pureconfig._
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.{blocking, Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.util.control.NoStackTrace
import upickle.default._

final case class KafkaSettings(
    queueNamePrefix: String,
    servers: String,
    groupId: String,
)

final case class Settings(
    etcdServer: String,
    shards: Int,
    namespace: String,
    nodeId: Int,
    leaderLeaseTtl: Long,
    nodeLeaseTtl: Long,
    kafka: KafkaSettings,
)

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val cf = ConfigFactory.load()

    implicit val sys = ActorSystem("ring", cf)
    implicit val mat = ActorMaterializer()
    import sys.dispatcher

    val config = loadConfig[Settings](cf.getConfig("ring")).valueOr(e => throw new IllegalArgumentException(e.toString))
    import config._

    args.headOption match {
      case Some("worker") =>
        val ps =
          ProducerSettings(sys, new StringSerializer, new StringSerializer).withBootstrapServers(kafka.servers)
        Source
          .tick(0.second, 1.second, ())
          .zipWithIndex
          .mapConcat {
            case (_, idx) =>
              logger.info(s"Send message#$idx to shards")
              Range.inclusive(1, shards).map { shard =>
                val queueName = s"${kafka.queueNamePrefix}$shard"
                new ProducerRecord[String, String](queueName, idx.toString)
              }
          }
          .runWith(Producer.plainSink(ps))
      case Some("node") =>
        // node settings
        val leaderKey = s"$namespace/leader"
        val keySeq = ByteSequence.fromString(leaderKey)
        val nodeIdKey = nodeId.toString
        val nodeIdSeq = ByteSequence.fromString(nodeIdKey)
        println(s"Watch $leaderKey key\nNode id: $nodeId")

        val client = Client.builder().endpoints(etcdServer).lazyInitialization(true).build()
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

        def lock(kvClient: KV, leaseClient: Lease, key: ByteSequence, ttl: Long): Future[Option[Long]] =
          for {
            grant <- leaseClient.grant(ttl).toScala
            leaseId = grant.getID()
            opt = PutOption.newBuilder().withLeaseId(leaseId).build()
            keyCmp = new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0))
            res <- kvClient.txn().If(keyCmp).Then(Op.put(key, nodeIdSeq, opt)).commit().toScala.transformWith {
              case Success(txnRes) if txnRes.isSucceeded => Future.successful(Some(leaseId))
              case _                                     => revoke(leaseClient, leaseId).map(_ => None)
            }
          } yield res

        val etcdEventsSource = Source
          .unfoldResourceAsync[List[EtcdEvent], Watcher](() => getWatcher(keySeq, client),
                                                         electionWatcher,
                                                         closeWatcher)
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

        def nodeStatusEvents() = {
          val flow = Source
            .tick(1.second, 1.second, Event.Tick)
            .buffer(size = 1, OverflowStrategy.dropHead)
            .merge(etcdEventsSource.map(Event.Etcd(_)))
            .prepend(Source.fromFuture(revokeLease().map(_ => Event.Init)))
            .scanAsync[NodeState](NodeState.Empty) {
              case (NodeState.Follower, Event.Etcd(EtcdEvent.Delete)) | (NodeState.Empty, Event.Init) =>
                lock(kvClient, leaseClient, keySeq, leaderLeaseTtl).map {
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
            .statefulMapConcat[NodeStatus] { () =>
              var _prevEvent = Option.empty[NodeStatus]

              { st: NodeState =>
                (st match {
                  case _: NodeState.Leader => Some(NodeStatus.Leader)
                  case NodeState.Follower  => Some(NodeStatus.Follower)
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
            .async
            .named("NodeStatus")
        }

        val nodesKeyPrefix = s"$namespace/nodes/"
        val nodesKeySeq = ByteSequence.fromString(nodesKeyPrefix)

        val nodeSettingsRegex = """^%s(\d+)/settings$""".format(nodesKeyPrefix, "%s").r

        val hypervisorSource =
          nodeStatusEvents
            .map(HypervisorEvent.Status(_))
            .merge {
              Source
                .tick(1.second, 1.second, HypervisorEvent.Tick)
                .buffer(size = 1, OverflowStrategy.dropHead)
            }
            .statefulMapConcat { () =>
              var _status = Option.empty[NodeStatus]

              {
                case HypervisorEvent.Status(status) =>
                  logger.info(s"Become a $status")
                  _status = Some(status)
                  Nil
                case t @ HypervisorEvent.Tick if _status.contains(NodeStatus.Leader) => List(t)
                case _                                                               => Nil
              }
            }
            .collect { case HypervisorEvent.Tick => () }
            .mapAsync(1) { _ =>
              kvClient
                .get(nodesKeySeq, GetOption.newBuilder().withPrefix(nodesKeySeq).build())
                .toScala
                .map(_.getKvs().asScala.toList.map { kv =>
                  kv.getKey().toStringUtf8() match {
                    case nodeSettingsRegex(id) =>
                      val sharding = read[NodeSharding](kv.getValue().toStringUtf8())
                      Some((id.toInt, (sharding, kv.getVersion())))
                    case _ => None
                  }
                }.flatten)
            }
            .async
            .mapAsync(1) { nodes => // TODO: optimize this step by scanAsync with state
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
                    case (acc @ (xs, ts), id) =>
                      val nodeId = id
                      val (sharding, version) = nodesMap.get(id).getOrElse((NodeSharding.empty, 0L))
                      val newRange = ranges(id - 1)
                      val intersect = sharding.range.intersect(newRange)

                      if (intersect.nonEmpty && !intersect.sameElements(newRange)) {
                        if (sharding.newRange.exists(_.sameElements(intersect))) acc
                        else (xs, (nodeId, sharding.copy(newRange = Some(intersect)), version) :: ts)
                      } else if (ts.isEmpty && !sharding.range.sameElements(newRange)) {
                        if (sharding.newRange.exists(_.sameElements(newRange))) acc
                        else ((nodeId, sharding.copy(newRange = Some(newRange)), version) :: xs, ts)
                      } else acc
                  }
                // if all nodes has only intersect shards as active then sync shards or else sync all with intersect shards only
                val nodesShards = if (intersectShards.nonEmpty) intersectShards else fullShards
                if (nodesShards.isEmpty) Future.unit
                else {
                  logger.info(s"Apply shards transactions: $nodesShards")
                  Future.traverse(nodesShards) {
                    case (nodeId, shard, version) =>
                      val keySeq = ByteSequence.fromString(s"$nodesKeyPrefix$nodeId/settings")
                      val cmp = new Cmp(keySeq, Cmp.Op.EQUAL, CmpTarget.version(version))
                      kvClient
                        .txn()
                        .If(cmp)
                        .Then(Op.put(keySeq, ByteSequence.fromString(write(shard)), PutOption.DEFAULT))
                        .commit()
                        .toScala
                        .map(_.isSucceeded)
                  }
                }
              } else Future.unit
            }
            .named("Hypervisor")

        hypervisorSource
          .runWith(Sink.ignore)
          .onComplete { res =>
            res match {
              case Success(_) =>
                logger.info(s"Node hypervisor is completed. Terminate system.")
              case Failure(e) =>
                logger.error(s"Node hypervisor has been failed. Terminate system.", e)
            }
            sys.terminate()
          }

        val nodeKeySeq = ByteSequence.fromString(s"$nodesKeyPrefix$nodeId/settings")

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
                  evt.getEventType() match {
                    case EventType.PUT =>
                      val sharding = read[NodeSharding](kv.getValue().toStringUtf8())
                      Some(RingNodeEvent.Sharding(sharding))
                    case (EventType.DELETE) => Some(RingNodeEvent.Reset)
                    case _                  => None
                  }
              }
              .flatten
            Some(xs)
          })

        val mergeSink: Sink[(Int, String), _] = MergeHub
          .source[(Int, String)]
          .toMat(Sink.foreach {
            case (shard, msg) =>
              println(s"Shard#$shard message#$msg")
          })(Keep.left)
          .run()

        def runShard(shard: Int): ShardCtl = {
          val queueName = s"${kafka.queueNamePrefix}$shard"
          val cs = ConsumerSettings(sys, new StringDeserializer, new StringDeserializer)
            .withBootstrapServers(kafka.servers)
            .withGroupId(kafka.groupId)
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

          val p = Promise[Unit]()
          val source = Consumer
            .plainSource(cs, Subscriptions.topics(queueName))
            .map(shard -> _.value)
          val switch = RestartSource
            .withBackoff(1.second, 1.second, 0)(() => source)
            .watchTermination() { (_, cb) =>
              cb.onComplete { _ =>
                p.success(())
              }
            }
            .viaMat(KillSwitches.single)(Keep.right)
            .named(s"Shard-$shard")
            .to(mergeSink)
            .run()
          ShardCtl(() => {
            switch.shutdown
            p.future
          })
        }

        def createNodeShard(key: ByteSequence): Future[TxnResponse] = {
          val nodeKeyCmp = new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0))
          kvClient
            .txn()
            .If(nodeKeyCmp)
            .Then(Op.put(key, ByteSequence.fromString(write(NodeSharding.empty)), PutOption.DEFAULT))
            .commit()
            .toScala
        }

        val nodeLockKeySeq = ByteSequence.fromString(s"$nodesKeyPrefix$nodeId/lock")
        lock(kvClient, leaseClient, nodeLockKeySeq, leaderLeaseTtl).foreach {
          case Some(leaseId) =>
            logger.info(s"Acquired lock#$leaseId for node#$nodeId")

            scala.sys.addShutdownHook(Await.result(revoke(leaseClient, leaseId), Duration.Inf))

            val lockSource = Source
              .tick(1.second, 1.second, Event.Tick)
              .buffer(size = 1, OverflowStrategy.dropHead)
              .mapAsync(1)(_ => leaseClient.keepAliveOnce(leaseId).toScala)

            val nodeEventsSource = Source
              .unfoldResourceAsync[List[RingNodeEvent], Watcher](() => getWatcher(nodeKeySeq, client),
                                                                 nodeWatcher,
                                                                 closeWatcher)
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
              .scanAsync(Map.empty[Int, ShardCtl]) {
                case (shardsMap, RingNodeEvent.Sharding(sharding)) =>
                  val newRange = sharding.newRange.getOrElse(sharding.range)
                  val shards = shardsMap.keys.toVector
                  logger.info(s"Apply sharding [newRange=$newRange] [shards=$shards]")
                  if (newRange.sameElements(shards)) Future.successful(shardsMap)
                  else {
                    val oldShards = shards.diff(newRange)
                    for {
                      _ <- Future.traverse(oldShards)(shard =>
                        shardsMap.get(shard).fold(Future.unit) { ctl =>
                          logger.info(s"Shutdown shard#$shard")
                          ctl.terminate()
                      })

                      newShards = newRange.diff(shards).map { shard =>
                        logger.info(s"Starting shard#$shard")
                        shard -> runShard(shard)
                      }

                      newMap = shardsMap -- oldShards ++ newShards

                      value = ByteSequence.fromString(write(sharding.copy(range = newMap.keys.toSeq, newRange = None)))
                      _ <- kvClient.put(nodeKeySeq, value).toScala
                    } yield newMap
                  }
                case (_, RingNodeEvent.Reset) =>
                  logger.error("Node shard settings has been deleted. Terminate system.")
                  sys.terminate().map(_ => Map.empty)
                case (shardsMap, _) => Future.successful(shardsMap)
              }

            lockSource
              .merge(nodeEventsSource)
              .runWith(Sink.ignore)
              .onComplete { res =>
                res match {
                  case Success(_) =>
                    logger.info(s"Node watcher is completed. Terminate system.")
                  case Failure(e) =>
                    logger.error(s"Node watcher has been failed. Terminate system.", e)
                }
                sys.terminate()
              }
          case _ =>
            logger.error(s"Cannot acquire a lock for node#$nodeId. Terminate system.")
            sys.terminate()
        }
      case cmd =>
        logger.error(s"Unknown command: $cmd")
        sys.terminate
    }

    Await.result(sys.whenTerminated, Duration.Inf)
    ()
  }
}

sealed trait NodeState
object NodeState {
  case object Empty extends NodeState
  case object Follower extends NodeState
  final case class Confirmation(leaseId: Long) extends NodeState
  final case class Leader(leaseId: Long) extends NodeState
}

sealed trait NodeStatus
object NodeStatus {
  case object Follower extends NodeStatus
  case object Leader extends NodeStatus
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

sealed trait HypervisorEvent
object HypervisorEvent {
  case object Tick extends HypervisorEvent
  final case class Status(event: NodeStatus) extends HypervisorEvent
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

case object CannotAcquireLock extends NoStackTrace

final case class ShardCtl(terminate: () => Future[Unit])
