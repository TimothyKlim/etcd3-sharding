object Hypervisor {
  /* Reshard nodes
   *
   * @param nodes sequence as (node id, (sharding settings, tx version))
   * @param shards length of shards range
   * @return resharded nodes sequence as (node id, sharding settings, tx version)
   */
  def reshard(nodes: Seq[(Int, (NodeSharding, Long))], shards: Int): List[(Int, NodeSharding, Long)] =
    if (nodes.nonEmpty) {
      val nodesCount: Int = nodes.map(_._1).max
      val nodesMap = nodes.toMap
      val rangeLength = shards / nodesCount.toDouble
      val ranges: Seq[Seq[Int]] = {
        val xs = Range.inclusive(1, shards).grouped(rangeLength.round.toInt).toIndexedSeq
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

            println(s"\n\nid#$id nodeId: $nodeId, sharding: $sharding, newRange: $newRange, intersect: $intersect")

            if (intersect.nonEmpty && !intersect.sameElements(newRange) &&
                !sharding.range.sameElements(intersect)) {
              println(s"Apply intersect for node#$nodeId")
              if (sharding.newRange.exists(_.sameElements(intersect))) acc
              else (xs, (nodeId, sharding.copy(newRange = Some(intersect)), version) :: ts)
            } else if (ts.isEmpty && !sharding.range.sameElements(newRange)) {
              println(s"Apply range for node#$nodeId")
              if (sharding.newRange.exists(_.sameElements(newRange))) acc
              else ((nodeId, sharding.copy(newRange = Some(newRange)), version) :: xs, ts)
            } else acc
        }
      // if all nodes has only intersect shards as active then sync shards or else sync all with intersect shards only
      println(s"\nintersectShards: $intersectShards\nfullShards: $fullShards\n")
      if (intersectShards.nonEmpty) intersectShards else fullShards
    } else Nil
}
