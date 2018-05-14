package app.metrics

import scala.collection.concurrent.TrieMap

object PrometheusMetric {
  val cache: TrieMap[MetricKey, String] = TrieMap.empty[MetricKey, String]

  def metricName(name: String, labels: Map[String, String]): String =
    cache.getOrElseUpdate((name, labels), s"${safeName(name)}${exposeLabels(labels)}")

  private def exposeLabels(labels: Map[String, String]): String =
    labels
      .map {
        case (k, v) if k.nonEmpty && v.nonEmpty ⇒ safeName(k) + "=\"" + v + '"'
        case _ ⇒ ""
      }
      .filter(_.nonEmpty)
      .mkString(",") match {
      case k if k.nonEmpty ⇒ '{' + k + '}'
      case _ ⇒ ""
    }

  private def safeName(s: String) = s.replace('.', '_').replace('-', '_')

  def render(name: String, sub: String, labels: Map[String, String], value: Int): String =
    render(name, sub, labels, value.toString)

  def render(name: String, sub: String, labels: Map[String, String], value: Double): String =
    render(name, sub, labels, value.toString)

  def render(name: String, sub: String, labels: Map[String, String], value: Long): String =
    render(name, sub, labels, value.toString)

  def render(name: String, sub: String, labels: Map[String, String], value: String): String =
    render(s"${name}_$sub", labels, value)

  def render(name: String, labels: Map[String, String], value: String): String =
    s"${metricName(name, labels)} $value\n"

  def render(dataPoint: MetricDataPointSet): String =
    List(
      render(dataPoint.name, "count", dataPoint.labels, dataPoint.snapshot.count),
      render(dataPoint.name, "size", dataPoint.labels, dataPoint.snapshot.size),
      render(dataPoint.name, "sum", dataPoint.labels, dataPoint.snapshot.sum),
      render(dataPoint.name, "min", dataPoint.labels, dataPoint.snapshot.min),
      render(dataPoint.name, "max", dataPoint.labels, dataPoint.snapshot.max),
      render(dataPoint.name, "mean", dataPoint.labels, dataPoint.snapshot.mean),
      render(dataPoint.name, "median", dataPoint.labels, dataPoint.snapshot.median),
      render(dataPoint.name, "stdDev", dataPoint.labels, dataPoint.snapshot.stdDev),
      render(dataPoint.name, "percentile75", dataPoint.labels, dataPoint.snapshot.percentile75),
      render(dataPoint.name, "percentile95", dataPoint.labels, dataPoint.snapshot.percentile95),
      render(dataPoint.name, "percentile98", dataPoint.labels, dataPoint.snapshot.percentile98),
      render(dataPoint.name, "percentile99", dataPoint.labels, dataPoint.snapshot.percentile99),
      render(dataPoint.name, "percentile999", dataPoint.labels, dataPoint.snapshot.percentile999)
    ).mkString
}
