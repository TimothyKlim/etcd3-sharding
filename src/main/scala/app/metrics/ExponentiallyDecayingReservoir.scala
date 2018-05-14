package app.metrics

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ConcurrentSkipListMap, ThreadLocalRandom}
import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.concurrent.duration._

object ExponentiallyDecayingReservoir {
  val DEFAULT_SIZE = 1028
  val DEFAULT_ALPHA = 0.015
  val RESCALE_THRESHOLD = 1.hour.toMillis

  /**
    * Creates a new `ExponentiallyDecayingReservoir` of 1028 elements, which offers a 99.9%
    * confidence level with a 5% margin of error assuming a normal distribution, and an alpha
    * factor of 0.015, which heavily biases the reservoir to the past 5 minutes of measurements.
    */
  def apply() = new ExponentiallyDecayingReservoir(DEFAULT_SIZE, DEFAULT_ALPHA)
}

/**
  * original: https://github.com/dropwizard/metrics/blob/master/metrics-core/src/main/java/com/codahale/metrics/ExponentiallyDecayingReservoir.java
  * Added accumulate function
  * <p/>
  * ------
  * <p/>
  * An exponentially-decaying random reservoir of `long`s. Uses Cormode et al's
  * forward-decaying priority reservoir sampling method to produce a statistically representative
  * sampling reservoir, exponentially biased towards newer entries.
  *
  * @see <a href="http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf">
  *      Cormode et al. Forward Decay: A Practical Time Decay Model for Streaming Systems. ICDE '09:
  *      Proceedings of the 2009 IEEE International Conference on Data Engineering (2009)</a>
  */
final class ExponentiallyDecayingReservoir(val size: Int, val alpha: Double) extends Reservoir {
  import ExponentiallyDecayingReservoir._

  private val values: concurrent.Map[Double, Long] = new ConcurrentSkipListMap[Double, Long].asScala
  private val count = new AtomicLong()
  private val lock = new ReentrantReadWriteLock()
  @volatile
  private var startTime = currentTime
  private val nextScaleTime = new AtomicLong(System.currentTimeMillis() + RESCALE_THRESHOLD)
  private val accumulateValue = new AtomicLong()

  private def currentTime = System.currentTimeMillis()

  override def update(value: Long, timestamp: Long = currentTime): Unit = {
    rescaleIfNeeded()
    lock.readLock().lock()
    try {
      val priority = weight(timestamp - startTime) / ThreadLocalRandom.current().nextDouble()
      val newCount = count.incrementAndGet()
      if (newCount <= size) values.put(priority, value)
      else {
        var first = values.head._1
        //noinspection ComparingUnrelatedTypes
        if (first < priority && values.putIfAbsent(priority, value) == null)
          while (values.remove(first) == null) first = values.head._1
      }
    } finally lock.readLock().unlock()
    accumulateValue.addAndGet(value)
    ()
  }

  override def snapshot: Snapshot = {
    lock.readLock().lock()
    try Snapshot(values.values, count.get())
    finally lock.readLock().unlock()
  }

  private def rescaleIfNeeded() = {
    val now = System.currentTimeMillis()
    val next = nextScaleTime.get()
    if (now >= next) rescale(now, next)
  }

  /**
    * "A common feature of the above techniques—indeed, the key technique that
    * allows us to track the decayed weights efficiently—is that they maintain
    * counts and other quantities based on g(ti − L), and only scale by g(t − L)
    * at query time. But while g(ti −L)/g(t−L) is guaranteed to lie between zero
    * and one, the intermediate values of g(ti − L) could become very large. For
    * polynomial functions, these values should not grow too large, and should be
    * effectively represented in practice by floating point values without loss of
    * precision. For exponential functions, these values could grow quite large as
    * new values of (ti − L) become large, and potentially exceed the capacity of
    * common floating point types. However, since the values stored by the
    * algorithms are linear combinations of g values (scaled sums), they can be
    * rescaled relative to a new landmark. That is, by the analysis of exponential
    * decay in Section III-A, the choice of L does not affect the final result. We
    * can therefore multiply each value based on L by a factor of exp(−α(L′ − L)),
    * and obtain the correct value as if we had instead computed relative to a new
    * landmark L′ (and then use this new L′ at query time). This can be done with
    * a linear pass over whatever data structure is being used."
    */
  private def rescale(now: Long, next: Long) =
    if (nextScaleTime.compareAndSet(next, now + RESCALE_THRESHOLD)) {
      lock.writeLock().lock()
      try {
        val oldStartTime = startTime
        startTime = currentTime
        for (key ← values.keys.toVector) {
          val value = values.remove(key).get
          values.put(key * Math.exp(-alpha * (startTime - oldStartTime)), value)
        }
        // make sure the counter is in sync with the number of stored samples.
        count.set(values.size.toLong)
      } finally lock.writeLock().unlock()
    }

  private def weight(time: Long) = Math.exp(alpha * time)
}
