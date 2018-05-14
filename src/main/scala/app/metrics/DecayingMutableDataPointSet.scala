package app.metrics

private[metrics] case class DecayingMutableDataPointSet(name: String, labels: Map[String, String]) {
  private val reservoir = ExponentiallyDecayingReservoir()

  def appendDataPoint(dataPoint: MetricDataPoint): Unit =
    reservoir.update(dataPoint.data, dataPoint.timestamp)

  def toImmutable: MetricDataPointSet =
    MetricDataPointSet(
      name = name,
      labels = labels,
      snapshot = reservoir.snapshot,
      timestamp = System.currentTimeMillis()
    )
}
