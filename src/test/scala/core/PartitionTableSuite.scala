package core

/**
  * Created by hminle on 12/2/2016.
  */
import core.Message.RegisterRequest
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PartitionTableSuite extends FunSuite with BeforeAndAfter{

  test("PartitionTable calculate KeyRange for 2 Slave"){
    val partitionTable: PartitionTable = PartitionTable(2)
    val registerRequests = List(
      RegisterRequest(1, "1", 1, List(Key(List(5)), Key(List(9)), Key(List(99)))),
      RegisterRequest(2, "2", 2, List(Key(List(10)), Key(List(7)), Key(List(50))))
    )
    registerRequests foreach(partitionTable registerSlave(_))
    partitionTable calculateKeyRange()
    val allSlaveKeyRange: List[SlaveKeyRange] = partitionTable.getAllSlaveKeyRange()
    assert(allSlaveKeyRange(0).keyRange === KeyRange(Key(List(5)), Key(List(10))))
  }

}
