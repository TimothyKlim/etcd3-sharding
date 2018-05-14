package app.metrics

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream._
import akka.stream.scaladsl._
import app._
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.util.control.NonFatal

final class MetricsExtension(settings: Settings)(implicit system: ActorSystem) extends Extension {
  private implicit val mat = ActorMaterializer()

  implicit val ec = system.dispatchers.lookup("metrics-dispatcher")

  private var activeCollectorMetrics = TrieMap.empty[MetricKey, DecayingMutableDataPointSet]
  private var prevCollectorMetrics = TrieMap.empty[MetricKey, DecayingMutableDataPointSet]

  private val metrics = TrieMap.empty[MetricKey, DefaultMeter]
  private val counters = TrieMap.empty[MetricKey, DefaultCounter]

  private val defaultLabels =
    settings.metrics.foldLeft(Map.empty[String, String])((m, s) => m + ("instance" -> s.prometheusGateway.instance))

  def meter(name: String, labels: Map[String, String] = Map.empty): Meter =
    metrics.getOrElseUpdate((name, labels), DefaultMeter(name, labels))

  def elapsedMeterMark(name: String,
                       labels: Map[String, String],
                       startAt: Long,
                       nextMark: Long = System.currentTimeMillis): Unit = {
    val elapsed = math.max(0L, nextMark - startAt)
    meter(name, labels).mark(elapsed)
    ()
  }

  def elapsedMeterMark[T](name: String, labels: Map[String, String])(f: => T): T = {
    val startAt = System.currentTimeMillis
    val res = f
    elapsedMeterMark(name, labels, startAt)
    res
  }

  def counter(name: String, labels: Map[String, String] = Map.empty): Counter =
    counters.getOrElseUpdate((name, labels), DefaultCounter(name, labels))

  def push(dataPoint: MetricDataPoint): Unit = {
    val labels = dataPoint.labels ++ defaultLabels
    activeCollectorMetrics
      .getOrElseUpdate(dataPoint.key, DecayingMutableDataPointSet(dataPoint.name, labels))
      .appendDataPoint(dataPoint.copy(labels = labels))
    ()
  }

  private def startReporter(): Unit = settings.metrics match {
    case Some(settings) =>
      println("Start metrics reporter")
      val httpFlow = Flow[(HttpRequest, Unit)]
        .buffer(settings.prometheusGateway.bufferSize, OverflowStrategy.dropTail)
        .via(
          Http(system)
            .cachedHostConnectionPool[Unit](settings.prometheusGateway.host, port = settings.prometheusGateway.port)
        )
      val backoffFlow = RestartFlow.withBackoff(
        minBackoff = settings.prometheusGateway.backoff.min,
        maxBackoff = settings.prometheusGateway.backoff.max,
        randomFactor = settings.prometheusGateway.backoff.randomFactor
      )(() => httpFlow)
      Source
        .tick(settings.interval, settings.interval, ())
        .map { _ =>
          val oldMetrics = prevCollectorMetrics
          prevCollectorMetrics = activeCollectorMetrics
          activeCollectorMetrics = TrieMap.empty[MetricKey, DecayingMutableDataPointSet]
          if (oldMetrics.isEmpty) None
          else {
            val body = oldMetrics.values.map(_.toImmutable).map(PrometheusMetric.render).mkString("\n")
            Some(HttpRequest(HttpMethods.POST, settings.prometheusGateway.uri, entity = HttpEntity(body)) -> (()))
          }
        }
        .collect { case Some(r) => r }
        .via(backoffFlow)
        .withAttributes(ActorAttributes.supervisionStrategy(decider))
        .runWith(Sink.ignore)
      ()
    case _ => ()
  }

  private val decider: Supervision.Decider = {
    case NonFatal(e) =>
      println(s"Decider failure: $e")
      Supervision.Resume
  }

  startReporter()

  private case class DefaultMeter(name: String, base: Map[String, String]) extends Meter {
    override def mark(value: Long, labels: Map[String, String] = Map.empty): Unit =
      push(MetricDataPoint(name, base ++ labels, System.currentTimeMillis(), value))

    override def timer(labels: Map[String, String] = Map.empty): Timer = DefaultTimer(labels)

    private case class DefaultTimer(labels: Map[String, String] = Map.empty) extends Timer {
      private val begin: Long = System.currentTimeMillis()
      override def stop(): Unit = {
        val end = System.currentTimeMillis() - begin
        DefaultMeter.this.mark(end.millis.toNanos, labels)
      }
    }
  }

  private case class DefaultCounter(name: String, labels: Map[String, String]) extends Counter {
    private val count = new AtomicLong()

    override def value: Long = count.get()
    override def inc(): Unit = {
      count.incrementAndGet()
      ()
    }
    override def dec(): Unit = {
      count.decrementAndGet()
      ()
    }
    override def set(value: Long): Unit = {
      count.set(value)
      ()
    }
  }
}
