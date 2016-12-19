package core

import java.net.InetAddress

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/**
  * Created by hminle on 11/9/2016.
  */

case class SlavePartition(slaveInfo: SlaveInfo, partition: List[(Key, Value)])

object Partitioner{
  def apply(): Partitioner = new Partitioner()
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}

class Partitioner()
  extends MessageHandler[Message] {
  import Partitioner.logger
  import Message._
  private val SLAVE_ADDRESS: String = InetAddress.getLocalHost.getHostAddress
  var partitionManager: MessageHandler[Message] = _
  var allSlaveKeyRange: List[SlaveKeyRange] = List.empty
  def setNextHandler(name: String, next: MessageHandler[Message]): Unit = {
    name match {
      case "PartitionManager" => partitionManager = next
      case _ => logger.error("Partitioner can only send messages to PartitionManager")
    }
  }

  def setSlaveKeyRange(slaveKeyRanges: List[SlaveKeyRange]): Unit = {
    allSlaveKeyRange = slaveKeyRanges
  }

  def handleMessage: Message => Unit = {
    case ChunkInfo(chunk) => partitionManager ! getPartitionedChunk(chunk)
    case _ => logger.error("Partitioner receives an unexpected Message")
  }

  def getPartitionedChunk(chunk: List[(Key, Value)]): Message = {
    logger.info("Partitioner receives chunkInfo")
    PartitionedChunk(performPartition(chunk))
  }

  def performPartition(chunk: List[(Key, Value)]): List[SlavePartition] = {
    val (minKeyRange: KeyRange, maxKeyRange: KeyRange) = getMinAndMaxKeyRange(allSlaveKeyRange)

    val allSlavePartition: List[SlavePartition] = allSlaveKeyRange map {
      slaveKeyRange => {
        val partition: List[(Key, Value)] = {
          if(slaveKeyRange.keyRange == minKeyRange) getPartition(isInMinKeyRange)(chunk, minKeyRange)
          else if(slaveKeyRange.keyRange == maxKeyRange) getPartition(isInMaxKeyRange)(chunk, maxKeyRange)
          else getPartition(isInKeyRange)(chunk, slaveKeyRange.keyRange)
        }

        SlavePartition(slaveKeyRange.slaveInfo, partition)
      }
    }
    val totalLengthOfMine: Int = allSlavePartition
      .filter(_.slaveInfo.address==SLAVE_ADDRESS)
      .foldLeft(0){
      (total, slavePar) => total + slavePar.partition.length
    }
    val totalLengthOfOthers: Int = allSlavePartition
      .filter(_.slaveInfo.address != SLAVE_ADDRESS)
      .foldLeft(0){
        (total, slavePar) => total + slavePar.partition.length
      }
    println(s"Partitioner check length of mine --> $totalLengthOfMine ---- length of others $totalLengthOfOthers")

    //Check whether or not some KeyValue left behind
    val partitionLeft: List[(Key, Value)] = chunk.diff(allSlavePartition.flatMap(_.partition))
    partitionLeft foreach(x => println(s"KEY --> ${x._1}"))
    assert(partitionLeft == List.empty)

    allSlavePartition
  }

  def getMinAndMaxKeyRange(allSlaveKeyRange: List[SlaveKeyRange]): (KeyRange, KeyRange) = {
    //TODO refactor min max KeyRange
    val minKeyRange: KeyRange = allSlaveKeyRange.foldLeft(allSlaveKeyRange(0).keyRange){
      (min, slaveKeyRange) =>
        if(min.beginKey < slaveKeyRange.keyRange.beginKey) min
        else slaveKeyRange.keyRange
    }
    val maxKeyRange: KeyRange = allSlaveKeyRange.foldLeft(allSlaveKeyRange(0).keyRange){
      (max, slaveKeyRange) =>
        if(max.endKey > slaveKeyRange.keyRange.endKey) max
        else slaveKeyRange.keyRange
    }
    (minKeyRange, maxKeyRange)
  }

  def getPartition(range:(KeyRange, Key)=> Boolean)(chunk: List[(Key, Value)],
                   keyRange: KeyRange): List[(Key,Value)] = {
    chunk.filter{keyValue => range(keyRange, keyValue._1)}
  }

  def isInKeyRange(keyRange: KeyRange, key: Key): Boolean = {
    key >= keyRange.beginKey && key < keyRange.endKey
  }

  def isInMinKeyRange(minKeyRange: KeyRange, key: Key): Boolean = {
    key < minKeyRange.endKey
  }

  def isInMaxKeyRange(maxKeyRange: KeyRange, key: Key): Boolean = {
    key >= maxKeyRange.beginKey
  }
}

