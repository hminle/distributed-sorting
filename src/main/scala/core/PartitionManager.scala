package core

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.generic.AtomicIndexFlag

/**
  * Created by hminle on 11/16/2016.
  */
object PartitionManager{
  def apply(slaveController: SlaveController,
            outputDir: String,
            hostAddress: String,
            port: Int,
            tempDir: String): PartitionManager =
    new PartitionManager(slaveController,
      outputDir,
      hostAddress,
      port,
      tempDir)
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}
class PartitionManager(slaveController: SlaveController,
                       outputDir: String,
                       hostAddress: String,
                       port: Int,
                       tempDir: String)
  extends MessageHandler[Message] {
  import PartitionManager.logger
  import Message._
  //TODO should change it to AtomicInteger???
  //TODO Change from 0 to 1
  private var partitionCounter: Int = 0
  private var fileNameCounter: AtomicInteger = new AtomicInteger(0)
  var totalNumOfSplitOfOtherSlaves: Int = _
  var sorter: MessageHandler[Message] = _
  var shuffler: MessageHandler[Message] = _
  // Initialize PartitionReceiver in another Thread
  val partitionReceiver: PartitionReceiver = new PartitionReceiver(this, hostAddress, port)
  val threadPartitionReceiver = new Thread(partitionReceiver)
  threadPartitionReceiver.start()


  def setTotalNumOfSplitOfOtherSlaves(num: Int): Unit ={
    totalNumOfSplitOfOtherSlaves = num
  }

  def setNextHandler(name: String, next: MessageHandler[Message]): Unit ={
    name match {
      case "Sorter" => sorter = next
      case "Shuffler" => shuffler = next
      case _ => logger.error("PartitionerManager can only send message to FileWriter and Shuffler")
    }
  }

  def handleMessage: Message => Unit = {
    case PartitionedChunk(allSlavePartition) =>  {
      logger.info("PartitionManager receives PartitionChunk")
      getShufflingPartitionInfo(allSlavePartition) foreach(message => shuffler ! message)
      sorter ! getMergingPartitionInfo(allSlavePartition)
    }
    case ShuffledPartitionInfo(id, split) => {
      logger.info(s"PartitionManager receives ${partitionCounter +1} ShuffledPartition / $totalNumOfSplitOfOtherSlaves")
      partitionCounter += 1

      val fileName: String = tempDir + "partitionFromOtherSlaves-" + fileNameCounter.get.toString
      fileNameCounter.incrementAndGet()
      sorter ! MergingPartitionInfo(fileName, split)

      if(partitionCounter == totalNumOfSplitOfOtherSlaves){
        logger.info( "PartitionManager receives enough splits")
        slaveController.setAllPartitionSentAndReceive
      }
    }
    case _ => logger.info("PartitionManager receives an unexpected Message")
  }

  def getMergingPartitionInfo(allSlavePartition: List[SlavePartition]): Message = {
    val slavePartition: SlavePartition = allSlavePartition.filter(_.slaveInfo.address == hostAddress).head
    val fileName: String = tempDir + "partition-" + fileNameCounter.get.toString
    fileNameCounter.incrementAndGet()
    MergingPartitionInfo(fileName, slavePartition.partition)
  }

  def getShufflingPartitionInfo(allSlavePartition: List[SlavePartition]): List[Message] = {
    for{
      slaveParition <- allSlavePartition
      if slaveParition.slaveInfo.address != hostAddress
    } yield ShufflingPartitionInfo(slaveParition.slaveInfo.address,
      slaveParition.slaveInfo.port,
      slaveParition.partition)
  }

  override def shutdown(): Unit = {
    logger.info("PartitionManager shutdowns PartitionReceiver")
    partitionReceiver receiveEnoughSplits()
  }
}
