package core

/**
  * Created by hminle on 10/27/2016.
  */

sealed trait Message

object Message{
  case class RegisterRequest(numOfInputSplits: Int, slaveSocketAddress: String, slavePort: Int,
                             sampleData: List[Key]) extends Message
  case class Confirmation() extends Message
  case class PartitionTableRequest() extends Message
  case class PartitionTableReturn(allSlaveKeyRange: List[SlaveKeyRange]) extends Message
  case class FinishNotification() extends Message
  case class FinishConfirmation() extends Message
  case class RequestFail() extends Message

  case class GetChunk() extends Message
  case class ChunkInfo(oneChunk: List[(Key, Value)]) extends Message


  case class SortedChunkInfo(sortedChunk: List[(Key, Value)]) extends Message


  case class PartitionedChunk(allSlavePartition: List[SlavePartition]) extends Message


  case class MergingPartitionInfo(fileName: String, partition: List[(Key, Value)]) extends Message
  case class SortedMergingPartitionInfo(fileName: String, partition: List[(Key, Value)]) extends Message

  case class ShufflingPartitionInfo(address: String, port: Int, partition: List[(Key, Value)]) extends Message


  case class ShuffledPartitionInfo(id: Int, partition: List[(Key, Value)]) extends Message
  case class ShuffledPartitionAck(id: Int) extends Message
  case class ShuffledPartitionFailed(address: String, port: Int, message: Message) extends Message



  import upickle.default._

  implicit val messageR: Reader[Message] = Reader[Message](
    implicitly[Reader[RegisterRequest]].read
      .orElse(implicitly[Reader[Confirmation]].read)
      .orElse(implicitly[Reader[PartitionTableRequest]].read)
      .orElse(implicitly[Reader[PartitionTableReturn]].read)
      .orElse(implicitly[Reader[FinishNotification]].read)
      .orElse(implicitly[Reader[FinishConfirmation]].read)
      .orElse(implicitly[Reader[RequestFail]].read)
      .orElse(implicitly[Reader[GetChunk]].read)
      .orElse(implicitly[Reader[ChunkInfo]].read)
      .orElse(implicitly[Reader[SortedChunkInfo]].read)
      .orElse(implicitly[Reader[PartitionedChunk]].read)
      .orElse(implicitly[Reader[MergingPartitionInfo]].read)
      .orElse(implicitly[Reader[SortedMergingPartitionInfo]].read)
      .orElse(implicitly[Reader[ShufflingPartitionInfo]].read)
      .orElse(implicitly[Reader[ShuffledPartitionInfo]].read)
      .orElse(implicitly[Reader[ShuffledPartitionAck]].read)
      .orElse(implicitly[Reader[ShuffledPartitionFailed]].read)
  )
  implicit val messageW: Writer[Message] = Writer[Message]{
    case x: RegisterRequest => writeJs(x)
    case x: Confirmation => writeJs(x)
    case x: PartitionTableRequest => writeJs(x)
    case x: PartitionTableReturn => writeJs(x)
    case x: FinishNotification => writeJs(x)
    case x: FinishConfirmation => writeJs(x)
    case x: RequestFail => writeJs(x)
    case x: GetChunk => writeJs(x)
    case x: ChunkInfo => writeJs(x)
    case x: SortedChunkInfo => writeJs(x)
    case x: PartitionedChunk => writeJs(x)
    case x: MergingPartitionInfo => writeJs(x)
    case x: SortedMergingPartitionInfo => writeJs(x)
    case x: ShufflingPartitionInfo => writeJs(x)
    case x: ShuffledPartitionInfo => writeJs(x)
    case x: ShuffledPartitionAck => writeJs(x)
    case x: ShuffledPartitionFailed => writeJs(x)
  }
}


