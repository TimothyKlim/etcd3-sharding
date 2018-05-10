import org.scalatest._

final class HypervisorSpec extends WordSpec with Matchers {
  private val shards = 16

  "Hypersvisor" must {
    "reshard empty nodes" in {
      val nodes = Hypervisor.reshard(Seq(
                                       (1, (NodeSharding.empty, 0))
                                     ),
                                     shards)
      nodes should contain theSameElementsAs Seq(
        (1, NodeSharding(Seq.empty, newRange = Some(range(1, shards))), 0)
      )
    }

    "reshard old nodes with new one" in {
      val fstIteration = Hypervisor.reshard(
        Seq(
          (1, (NodeSharding(range(1, 4), None), 0)),
          (2, (NodeSharding(range(5, 8), None), 0)),
          (3, (NodeSharding(range(9, 12), None), 0)),
          (4, (NodeSharding(range(13, 16), None), 0)),
          (5, (NodeSharding.empty, 0)),
        ),
        shards
      )
      fstIteration should contain theSameElementsAs Seq(
        (1, NodeSharding(Seq.empty, newRange = Some(range(1, 3))), 0),
        (2, NodeSharding(Seq.empty, newRange = Some(Seq(5, 6))), 0),
        (3, NodeSharding(Seq.empty, newRange = Some(Seq(9))), 0)
      )
      val sndIteration = Hypervisor.reshard(fstIteration.map {
        case (id, sharding, version) => (id, (sharding, version))
      }, shards)
      sndIteration should contain theSameElementsAs Seq(
        (1, NodeSharding(Seq.empty, newRange = Some(range(1, 3))), 0),
        (2, NodeSharding(Seq.empty, newRange = Some(range(4, 6))), 0),
        (3, NodeSharding(Seq.empty, newRange = Some(range(7, 9))), 0),
        (4, NodeSharding(Seq.empty, newRange = Some(range(10, 12))), 0),
        (5, NodeSharding(Seq.empty, newRange = Some(range(13, 16))), 0)
      )
    }
  }

  private def range(from: Int, to: Int): Seq[Int] =
    Range.inclusive(from, to)
}
