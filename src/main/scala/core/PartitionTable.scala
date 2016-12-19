package core

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.collection.immutable.HashMap.HashMap1
import scala.collection.mutable.ListBuffer
import Message._
/**
  * Created by hminle on 10/27/2016.
  */
object PartitionTable {
  def apply(numOfSlaves: Int): PartitionTable = new PartitionTableImp(numOfSlaves)
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}
trait PartitionTable {
  def registerSlave(request: RegisterRequest): Unit
  def calculateKeyRange(): Unit
  def getAllSlavesAddress(): String
  def getAllSlaveKeyRange(): List[SlaveKeyRange]
}

class PartitionTableImp(numOfSlaves: Int)
  extends PartitionTable {
  import PartitionTable.logger
  val allSlaveAddress: ListBuffer[String] = ListBuffer.empty[String]
  val allSlaveInfoBuffer: ListBuffer[SlaveInfo] = ListBuffer.empty[SlaveInfo]
  var allSlaveInfo: List[SlaveInfo] = List.empty
  var keyRange: List[Key] = List.empty
  var allSlaveKeyRange: List[SlaveKeyRange] = List.empty

  def registerSlave(request: RegisterRequest): Unit = {
    val slaveInfo: SlaveInfo = SlaveInfo(request.slaveSocketAddress,
      request.slavePort,
      request.numOfInputSplits)
    logger.info("PartitionTable register SlaveInfo "+ slaveInfo.address)
    allSlaveAddress += request.slaveSocketAddress
    allSlaveInfoBuffer += slaveInfo
    keyRange = keyRange:::request.sampleData
  }

  def calculateKeyRange(): Unit = {
    logger.info("Calculate KeyRange for all Slaves")
    allSlaveAddress.toList
    allSlaveInfo = allSlaveInfoBuffer.toList
    val allSlaveKeyRangeBuffer: ListBuffer[SlaveKeyRange] = ListBuffer.empty[SlaveKeyRange]
    val sortedKeyRange = keyRange sortWith(_ < _)
    var counter: Int = 0
    var slaveInfoIndex: Int = 0
    val step: Int = sortedKeyRange.length / numOfSlaves
    val lengthOfKeys: Int = sortedKeyRange.length
    while(counter < lengthOfKeys){
      val beginKey: Key = sortedKeyRange(counter)
      val endKey: Key = {
        if((counter + step) >= lengthOfKeys) sortedKeyRange(lengthOfKeys - 1)
        else sortedKeyRange(counter + step)
      }
      val keyRange: KeyRange = KeyRange(beginKey, endKey)
      val slaveKeyRange: SlaveKeyRange = SlaveKeyRange(allSlaveInfo(slaveInfoIndex), keyRange)
      allSlaveKeyRangeBuffer += slaveKeyRange
      slaveInfoIndex += 1
      counter += step
    }
    allSlaveKeyRange = allSlaveKeyRangeBuffer.toList
  }

  def getAllSlavesAddress(): String = {
    allSlaveAddress.mkString(", ")
  }
  def getAllSlaveKeyRange(): List[SlaveKeyRange] = {
    logger.debug(s"PRINT Slave Key range $allSlaveKeyRange")
    allSlaveKeyRange
  }

}
case class KeyRange(beginKey: Key, endKey: Key)
case class SlaveInfo(address: String, port: Int, slaveNumOfsplits: Int)
case class SlaveKeyRange(slaveInfo: SlaveInfo, keyRange: KeyRange)

