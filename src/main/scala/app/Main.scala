package app

import akka.actor._
import akka.Done
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.scaladsl._
import akka.kafka._
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, KillSwitches, OverflowStrategy}
import app.metrics.MetricsExtension
import cats.effect.IO
import cats.syntax.either._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import doobie._, doobie.implicits._, doobie.hikari._
import io.etcd.jetcd.data._
import io.etcd.jetcd.kv.TxnResponse
import io.etcd.jetcd.op._
import io.etcd.jetcd.options._
import io.etcd.jetcd.Watch._
import io.etcd.jetcd.watch._, WatchEvent.EventType
import io.etcd.jetcd._
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import pureconfig._
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration._
import scala.concurrent.{blocking, Await, Future, Promise}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}
import com.google.common.base.Charsets.UTF_8
import upickle.default._

final case class KafkaSettings(
    queueNamePrefix: String,
    servers: String,
    groupId: String,
)

final case class JdbcSettings(
    url: String,
    user: String,
    password: String,
)

final case class BackoffSettings(min: FiniteDuration, max: FiniteDuration, randomFactor: Double)

final case class PrometheusGatewaySettings(host: String,
                                           port: Int,
                                           uri: String,
                                           instance: String,
                                           bufferSize: Int,
                                           backoff: BackoffSettings)

final case class MetricsSettings(prometheusGateway: PrometheusGatewaySettings, interval: FiniteDuration)

final case class Settings(
    etcdServer: String,
    shards: Int,
    namespace: String,
    nodeId: Int,
    leaderLeaseTtl: Long,
    nodeLeaseTtl: Long,
    kafka: KafkaSettings,
    jdbc: JdbcSettings,
    metrics: Option[MetricsSettings],
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
        (for {
          tx <- HikariTransactor
            .newHikariTransactor[IO]("org.postgresql.Driver", jdbc.url, jdbc.user, jdbc.password)
          _ = logger.info("Truncate items table")
          _ <- ItemsRepo.migrate().transact(tx)
        } yield tx).attempt.unsafeRunSync match {
          case Left(e) =>
            logger.error("DB failure", e)
            sys.terminate
            throw e
          case _ =>
        }

        val ps =
          ProducerSettings(sys, new StringSerializer, new StringSerializer).withBootstrapServers(kafka.servers)
        val count = 50
        Source
          .repeat(())
          .zipWithIndex
          .mapConcat {
            case (_, idx) =>
              logger.info(s"Send $count messages#$idx to shards")
              Range.inclusive(1, shards).flatMap { shard =>
                val queueName = s"${kafka.queueNamePrefix}$shard"
                Range(0, count).map { offset =>
                  new ProducerRecord[String, String](queueName, (count * idx + offset).toString)
                }
              }
          }
          .runWith(RestartSink.withBackoff(minBackoff = 1.second, maxBackoff = 3.seconds, randomFactor = 0.25)(() =>
            Producer.plainSink(ps)))
      case Some("node") =>
        val metricsExt = new MetricsExtension(config)(sys)

        implicit val xa = Await.result(
          HikariTransactor
            .newHikariTransactor[IO]("org.postgresql.Driver", jdbc.url, jdbc.user, jdbc.password)
            .attempt
            .unsafeToFuture,
          Duration.Inf
        ) match {
          case Right(tx) => tx
          case Left(e) =>
            logger.error("DB failure", e)
            sys.terminate
            throw e
        }

        // node settings
        val leaderKey = s"$namespace/leader"
        val keySeq = ByteSequence.from(leaderKey, UTF_8)
        val nodeIdKey = nodeId.toString
        val nodeIdSeq = ByteSequence.from(nodeIdKey, UTF_8)
        println(s"Watch $leaderKey key\nNode id: $nodeId")

        val client = Client.builder().endpoints(etcdServer).lazyInitialization(true).build()
        val kvClient = client.getKVClient()
        val leaseClient = client.getLeaseClient()

        def getWatcher(key: ByteSequence, client: Client, revision: Long = 0): Future[Watcher] = {
          val watchOpt = WatchOption.newBuilder().withRevision(revision).build()
          Future(blocking(client.getWatchClient().watch(key, watchOpt)))
        }

        def electionWatcher(w: Watcher): Future[Option[List[EtcdEvent]]] =
          Future(blocking {
            val xs = w
              .listen()
              .getEvents()
              .asScala
              .toList
              .map { evt =>
                val kv = evt.getKeyValue()
                if (kv.getKey().toString(UTF_8) != leaderKey) None
                else
                  evt.getEventType() match {
                    case EventType.PUT          => Some(EtcdEvent.Put(kv.getValue().toString(UTF_8)))
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
        val nodesKeySeq = ByteSequence.from(nodesKeyPrefix, UTF_8)

        val nodeSettingsRegex = """^%s(\d+)/settings$""".format(nodesKeyPrefix, "%s").r

        def nodeUpdatesSource(): Source[HypervisorEvent, _] =
          Source
            .unfoldResourceAsync[List[HypervisorEvent], Watcher](
              () => {
                val watchOpt = WatchOption.newBuilder().withPrefix(nodesKeySeq).build()
                Future(blocking(client.getWatchClient().watch(nodesKeySeq, watchOpt)))
              }, { w: Watcher =>
                Future(blocking(Some {
                  w.listen()
                    .getEvents()
                    .asScala
                    .map(_.getKeyValue().getKey().toString(UTF_8) match {
                      case nodeSettingsRegex(_) => Some(HypervisorEvent.NodesUpdated)
                      case _                    => None
                    })
                    .flatten
                    .toList
                }))
              },
              closeWatcher
            )
            .mapConcat(identity)

        def getNodes(): Future[List[Hypervisor.NodeItem]] = {
          logger.info("Get nodes from etcd")
          kvClient
            .get(nodesKeySeq, GetOption.newBuilder().withPrefix(nodesKeySeq).build())
            .toScala
            .map(_.getKvs().asScala.toList.map { kv =>
              kv.getKey().toString(UTF_8) match {
                case nodeSettingsRegex(id) =>
                  val sharding = read[NodeSharding](kv.getValue().toString(UTF_8))
                  Some((id.toInt, sharding, kv.getVersion()))
                case _ => None
              }
            }.flatten)
        }

        val hypervisorSource =
          nodeStatusEvents
            .map(HypervisorEvent.Status(_))
            .merge(nodeUpdatesSource)
            .statefulMapConcat { () =>
              var _status = Option.empty[NodeStatus]

              {
                case HypervisorEvent.Status(status) =>
                  logger.info(s"Become a $status")
                  _status = Some(status)
                  List(HypervisorEvent.NodesUpdated)
                case t @ HypervisorEvent.NodesUpdated if _status.contains(NodeStatus.Leader) => List(t)
                case _                                                                       => Nil
              }
            }
            .mapAsync(1) {
              case HypervisorEvent.NodesUpdated =>
                getNodes().flatMap { nodes =>
                  if (nodes.nonEmpty) {
                    val nodesShards = Hypervisor.reshard(nodes, nodes.map(_._1).max, shards)
                    logger.info(s"Nodes shards: $nodesShards")
                    if (nodesShards.isEmpty) Future.unit
                    else {
                      Future.traverse(nodesShards) {
                        case (nodeId, shard, version) =>
                          val keySeq = ByteSequence.from(s"$nodesKeyPrefix$nodeId/settings", UTF_8)
                          val cmp = new Cmp(keySeq, Cmp.Op.EQUAL, CmpTarget.version(version))
                          kvClient
                            .txn()
                            .If(cmp)
                            .Then(Op.put(keySeq, ByteSequence.from(write(shard), UTF_8), PutOption.DEFAULT))
                            .commit()
                            .toScala
                            .map(_.isSucceeded)
                      }
                    }
                  } else Future.unit
                }
              case evt =>
                logger.error(s"Unknown event: $evt")
                Future.unit
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

        val nodeKeySeq = ByteSequence.from(s"$nodesKeyPrefix$nodeId/settings", UTF_8)

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
                      val sharding = read[NodeSharding](kv.getValue().toString(UTF_8))
                      Some(RingNodeEvent.Sharding(sharding))
                    case (EventType.DELETE) => Some(RingNodeEvent.Reset)
                    case _                  => None
                  }
              }
              .flatten
            Some(xs)
          })

        val writes = new AtomicLong()

        lazy val metricsFlow = config.metrics match {
          case Some(metrics) =>
            Source
              .tick(metrics.interval, metrics.interval, ())
              .map { _ =>
                val count = writes.get()
                val authsCounter = metricsExt.counter("etc-shard-writes", Map("node-id" -> nodeId.toString))
                authsCounter.set(count)
                logger.info(s"Writes: ${count / metrics.interval.toSeconds} ps")
                writes.set(0L)
              }
              .runWith(Sink.ignore)
          case _ => ()
        }

        val nodeSink: Sink[(Int, (String, CommittableOffset)), _] =
          Flow[(Int, (String, CommittableOffset))]
            .mapAsync(1) {
              case (shard, (msg, offset)) =>
                for {
                  _ <- ItemsRepo.create(Item(shard, msg)).transact(xa).unsafeToFuture()
                  _ <- offset.commitScaladsl()
                } yield writes.incrementAndGet()
            }
            .watchTermination() { (mat, cb) =>
              cb.onComplete {
                case Success(_) =>
                  logger.info(s"Shard sink has been completed.")
                case Failure(e) =>
                  logger.error(s"Shard sink has been failed. Terminate system.", e)
                  sys.terminate()
              }
              mat
            }
            .toMat(Sink.ignore)(Keep.left)

        def runShard(shard: Int): ShardCtl = {
          val queueName = s"${kafka.queueNamePrefix}$shard"
          val cs = ConsumerSettings(sys, new StringDeserializer, new StringDeserializer)
            .withBootstrapServers(kafka.servers)
            .withGroupId(kafka.groupId)
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

          val p = Promise[Unit]()
          val source = Consumer
            .committableSource(cs, Subscriptions.topics(queueName))
            .map(m => (shard, (m.record.value, m.committableOffset)))
          val switch = RestartSource
            .withBackoff(1.second, 1.second, 0)(() => source)
            .watchTermination() { (_, cb) =>
              cb.onComplete { _ =>
                p.success(())
              }
            }
            .viaMat(KillSwitches.single)(Keep.right)
            .named(s"Shard-$shard")
            .to(nodeSink)
            .run()
          metricsFlow
          ShardCtl(() => {
            switch.shutdown
            p.future
          })
        }

        def createOrGetNodeShard(key: ByteSequence): Future[TxnResponse] = {
          val nodeKeyCmp = new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0))
          kvClient
            .txn()
            .If(nodeKeyCmp)
            .Then(Op.put(key, ByteSequence.from(write(NodeSharding.empty), UTF_8), PutOption.DEFAULT))
            .Then(Op.get(key, GetOption.DEFAULT))
            .Else(Op.get(key, GetOption.DEFAULT))
            .commit()
            .toScala
        }

        val nodeLockKeySeq = ByteSequence.from(s"$nodesKeyPrefix$nodeId/lock", UTF_8)
        lock(kvClient, leaseClient, nodeLockKeySeq, leaderLeaseTtl).foreach {
          case Some(leaseId) =>
            logger.info(s"Acquired lock#$leaseId for node#$nodeId")

            scala.sys.addShutdownHook(Await.result(revoke(leaseClient, leaseId), Duration.Inf))

            val lockSource = Source
              .tick(1.second, 1.second, Event.Tick)
              .buffer(size = 1, OverflowStrategy.dropHead)
              .mapAsync(1)(_ => leaseClient.keepAliveOnce(leaseId).toScala)

            val nodeEventsSource =
              Source
                .fromFuture(createOrGetNodeShard(nodeKeySeq).map { res =>
                  val getRes = res.getGetResponses().asScala.flatMap(_.getKvs.asScala).head
                  if (res.isSucceeded) (RingNodeEvent.Sharding.empty, getRes.getModRevision())
                  else {
                    val sharding = read[NodeSharding](getRes.getValue().toString(UTF_8))
                    (RingNodeEvent.Sharding(sharding), getRes.getModRevision())
                  }
                })
                .flatMapConcat {
                  case (evt, revision) =>
                    Source
                      .unfoldResourceAsync[List[RingNodeEvent], Watcher](() => getWatcher(nodeKeySeq, client, revision),
                                                                         nodeWatcher,
                                                                         closeWatcher)
                      .mapConcat(identity)
                      .prepend(Source.single(evt))
                }
                .scanAsync(Map.empty[Int, ShardCtl]) {
                  case (shardsMap, RingNodeEvent.Sharding(sharding)) =>
                    val newRange = sharding.newRange.getOrElse(sharding.range)
                    val shards = shardsMap.keys.toSet
                    if (newRange == shards && sharding.newRange.isEmpty) Future.successful(shardsMap)
                    else {
                      logger.info(s"Apply sharding [sharding=$sharding] to [shards=$shards]")
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

                        value = write(NodeSharding(range = newMap.keys.toSet, newRange = None))
                        _ <- kvClient.put(nodeKeySeq, ByteSequence.from(value, UTF_8)).toScala
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
  case object NodesUpdated extends HypervisorEvent
  final case class Status(event: NodeStatus) extends HypervisorEvent
}

final case class NodeSharding(
    range: Set[Int], // actual node shard range
    newRange: Option[Set[Int]] // shard range to apply from a leader
)
object NodeSharding {
  val empty = NodeSharding(Set.empty, None)

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

final case class Item(shard: Int, msg: String)
object ItemsRepo {
  def migrate(): ConnectionIO[Unit] =
    for {
      _ <- sql"CREATE TABLE IF NOT EXISTS items (shard int NOT NULL, msg text NOT NULL, PRIMARY KEY (shard, msg))".update.run
      _ <- sql"TRUNCATE TABLE items".update.run
    } yield ()

  def create(item: Item): ConnectionIO[Int] =
    sql"INSERT INTO items (shard, msg) VALUES (${item.shard}, ${item.msg})".update.run
}
