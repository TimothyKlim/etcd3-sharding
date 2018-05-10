object Hypervisor {
  type NodeItem = (Int, NodeSharding, Long) // (node id, sharding settings, tx version)

  /* Reshard nodes
   *
   * @param nodes sequence as (node id, sharding settings, tx version)
   * @param shards length of shards range
   * @return resharded nodes sequence as (node id, sharding settings, tx version)
   */
  def reshard(nodes: Seq[NodeItem], nodesCount: Int, shardsCount: Int): List[NodeItem] =
    if (nodes.nonEmpty && nodes.forall(_._2.newRange.isEmpty)) {
      val nodesMap = nodes.map { case (id, sharding, version) => (id, (sharding, version)) }.toMap
      val rangeLength = shardsCount / nodesCount.toDouble
      val newRanges: Seq[Set[Int]] = {
        val xs = Range.inclusive(1, shardsCount).grouped(rangeLength.round.toInt).toIndexedSeq
        if (rangeLength == rangeLength.toInt) xs
        else xs.dropRight(2) ++ Seq(xs.takeRight(2).flatten) // merge last chunk into single
      }.map(_.toSet)

      val (intersect, newest) = newRanges.zipWithIndex.foldLeft((List.empty[NodeItem], List.empty[NodeItem])) {
        case ((intBuf, newBuf), (newRange, idx)) =>
          val nodeId = idx + 1
          val (sharding, version) = nodesMap.get(nodeId).getOrElse((NodeSharding.empty, 0L))
          val intersect = newRange.intersect(sharding.range)

          val xs =
            if (sharding.range == intersect) intBuf
            else (nodeId, sharding.copy(newRange = Some(intersect)), version) :: intBuf

          val ys =
            if (intBuf.nonEmpty || sharding.range == newRange) newBuf
            else (nodeId, sharding.copy(newRange = Some(newRange)), version) :: newBuf

          (xs, ys)
      }
      if (intersect.nonEmpty) intersect else newest
    } else Nil
}
