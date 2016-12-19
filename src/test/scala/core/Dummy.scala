package core
import Message._
/**
  * Created by hminle on 11/21/2016.
  */
object DummyData {
  val (key1, key2, key3, key4, key5, key6, key7, key8, key9) =
    (Key(List(1)), Key(List(2)), Key(List(3)), Key(List(4)), Key(List(5)),
      Key(List(6)), Key(List(7)), Key(List(8)), Key(List(9)))
  val slaveKey1 = List(key1, key2, key3)
  val slaveKey2 = List(key4, key5, key6)
  val slaveKey3 = List(key7, key8, key9)
  val registerRequests: List[RegisterRequest] =
    List(RegisterRequest(1, "1", 8511, slaveKey1),
      RegisterRequest(2, "2", 8511, slaveKey2),
      RegisterRequest(3, "3", 8511, slaveKey3))
  val partitionTable: PartitionTable = PartitionTable(3)
  registerRequests foreach(partitionTable registerSlave(_))
  partitionTable calculateKeyRange()
  val allSlaveKeyRange: List[SlaveKeyRange] = partitionTable.getAllSlaveKeyRange()
  val keys: List[Key] = List(Key(List(2)), Key(List(1)), Key(List(5)), Key(List(4)), Key(List(3)),
    Key(List(6)), Key(List(9)), Key(List(8)), Key(List(7)))
  val sortedKeys: List[Key] = List(Key(List(1)), Key(List(2)), Key(List(3)), Key(List(4)), Key(List(5)),
    Key(List(6)), Key(List(7)), Key(List(8)), Key(List(9)))

  val emptyValue: Value = Value(List.empty)
  val unsortedData: List[(Key, Value)] = for(key <- keys) yield (key, emptyValue)
  val sortedData: List[(Key, Value)] = for(key <- sortedKeys) yield (key, emptyValue)
}
class DummyPartitioner(allSlaveKeyRange: List[SlaveKeyRange])
  extends Partitioner() {
  override def handleMessage: Message => Unit = {case r: Message => {}}

}