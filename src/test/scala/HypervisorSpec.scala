import org.scalatest._

final class HypervisorSpec extends WordSpec with Matchers {
  private val shardsCount = 16

  "Hypersvisor" must {
    "reshard empty nodes" in {
      val nodes = Hypervisor.reshard(Seq(
                                       (1, NodeSharding.empty, 0)
                                     ),
                                     nodesCount = 1,
                                     shardsCount = shardsCount)
      nodes should contain theSameElementsAs Seq(
        (1, NodeSharding(Seq.empty, newRange = Some(range(1, shardsCount))), 0)
      )
    }

    "reshard old nodes with new one" in {
      val fstIteration = Hypervisor
        .reshard(
          Seq(
            (1, NodeSharding(range(1, 4), None), 0),
            (2, NodeSharding(range(5, 8), None), 0),
            (3, NodeSharding(range(9, 12), None), 0),
            (4, NodeSharding(range(13, 16), None), 0),
            (5, NodeSharding.empty, 0),
          ),
          nodesCount = 5,
          shardsCount = shardsCount
        )
        .sortBy(_._1)
      fstIteration should contain theSameElementsAs Seq(
        (1, NodeSharding(range(1, 4), newRange = Some(range(1, 3))), 0),
        (2, NodeSharding(range(5, 8), newRange = Some(Seq(5, 6))), 0),
        (3, NodeSharding(range(9, 12), newRange = Some(Seq(9))), 0),
        (4, NodeSharding(range(13, 16), newRange = Some(Seq.empty)), 0)
      )
      val sndIteration = Hypervisor
        .reshard(
          fstIteration.map {
            case (nodeId, NodeSharding(_, Some(newRange)), version) =>
              (nodeId, NodeSharding(newRange, None), version)
          },
          nodesCount = 5,
          shardsCount = shardsCount
        )
        .sortBy(_._1)
      sndIteration should contain theSameElementsAs Seq(
        (2, NodeSharding(Seq(5, 6), newRange = Some(range(4, 6))), 0),
        (3, NodeSharding(Seq(9), newRange = Some(range(7, 9))), 0),
        (4, NodeSharding(Seq.empty, newRange = Some(range(10, 12))), 0),
        (5, NodeSharding(Seq.empty, newRange = Some(range(13, 16))), 0)
      )
    }
  }

  private def range(from: Int, to: Int): Seq[Int] =
    Range.inclusive(from, to)
}
