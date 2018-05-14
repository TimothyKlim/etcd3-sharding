package app.metrics

object MetricDataPoint {
  def apply(name: String, timestamp: Long, data: Long): MetricDataPoint =
    MetricDataPoint(name = name, labels = Map.empty, timestamp = timestamp, data = data)
}

final case class MetricDataPoint(name: String, labels: Map[String, String], timestamp: Long, data: Long) {
  def key = (name, labels)
}

trait Reservoir {
  def update(value: Long, timestamp: Long): Unit
  def snapshot: Snapshot
}

object Snapshot {
  def apply(values: Iterable[Long], count: Long): Snapshot = apply(values.toArray, count)
  def apply(values: Array[Long], count: Long): Snapshot = new Snapshot(values.sorted, count)
}

final class Snapshot(private val values: Array[Long], val count: Long) {
  def size: Int = values.length
  def sum: Long = values.sum
  def median: Double = percentile(0.5)
  def percentile75: Double = percentile(0.75)
  def percentile95: Double = percentile(0.95)
  def percentile98: Double = percentile(0.98)
  def percentile99: Double = percentile(0.99)
  def percentile999: Double = percentile(0.999)
  def min: Long = values.lastOption.getOrElse(0)
  def max: Long = values.headOption.getOrElse(0)
  def mean: Double = sum.toDouble / values.length
  def stdDev: Double = {
    if (values.length <= 1) {
      return 0
    }
    val m = mean
    val sum = values.foldLeft(0.0) { (sum, value) ⇒
      val diff = value - m
      sum + diff * diff
    }
    val variance = sum / (values.length - 1)
    Math.sqrt(variance)
  }

  private def percentile(quantile: Double): Double = {
    if (quantile < 0.0 || quantile > 1.0) throw new IllegalArgumentException(quantile + " is not in [0..1]")
    if (values.isEmpty) 0.0
    else {
      val pos = quantile * (values.length + 1)
      if (pos < 1) values.head.toDouble
      else if (pos >= values.length) values.last.toDouble
      else {
        val lower = values(pos.toInt - 1).toDouble
        val upper = values(pos.toInt).toDouble
        lower + (pos - Math.floor(pos)) * (upper - lower)
      }
    }
  }
}

trait Meter {
  def mark(value: Long = 1, labels: Map[String, String] = Map.empty): Unit
  def timer(labels: Map[String, String] = Map.empty): Timer
}

trait Timer {
  def stop(): Unit
}

trait Counter {
  def value: Long
  def inc(): Unit
  def dec(): Unit
  def set(value: Long): Unit
}

final case class MetricDataPointSet(name: String, labels: Map[String, String], snapshot: Snapshot, timestamp: Long)
