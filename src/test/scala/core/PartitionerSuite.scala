package core

/**
  * Created by hminle on 11/21/2016.
  */
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PartitionerSuite extends FunSuite{
  import DummyData._

  val partitioner: Partitioner = Partitioner()
  partitioner.setSlaveKeyRange(allSlaveKeyRange)
  val(minKeyRange, maxKeyRange) = partitioner.getMinAndMaxKeyRange(allSlaveKeyRange)
  val allSlavePartition: List[SlavePartition] = partitioner performPartition(sortedData)

  test("Partitioner gives Min and Max KeyRange"){
    assert(minKeyRange === KeyRange(Key(List(1)), Key(List(4))))
    assert(maxKeyRange === KeyRange(Key(List(7)), Key(List(9))))
  }
  test("Partitioner isInMinKeyRange works correctly"){
    assert(partitioner.isInMinKeyRange(minKeyRange, Key(List(2))))
    assert(partitioner.isInMaxKeyRange(maxKeyRange, Key(List(8))))
  }
  test("Partitioner should partition Chunk properly"){
    assert(allSlavePartition(0).partition === sortedData.take(3))
  }
  test("Check there should be no partition left"){
    val partitionLeft: List[(Key,Value)] = sortedData.diff(allSlavePartition.flatMap(_.partition))
    assert(partitionLeft === List.empty)
  }

}
