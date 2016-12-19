package core

import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.typesafe.scalalogging.Logger
import common.Utils
import core.Message.{ShuffledPartitionAck, ShuffledPartitionFailed, ShuffledPartitionInfo, ShufflingPartitionInfo}
import org.slf4j.LoggerFactory

/**
  * Created by hminle on 11/17/2016.
  */
object Shuffler {
  def apply(slaveController: SlaveController, totalNumOfSplitNeedToSendOut: Int): Shuffler =
    new Shuffler(slaveController,totalNumOfSplitNeedToSendOut)
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}
class Shuffler(slaveController: SlaveController, totalNumOfSplitNeedToSendOut: Int)
  extends MessageHandler[Message] {
  import Shuffler.logger

  private val NUM_OF_THREADS: Int = 10
  private val executor: ExecutorService = Executors.newFixedThreadPool(NUM_OF_THREADS)
  private val currentID: AtomicInteger = new AtomicInteger(0)
  private val numOfSentPartitions: AtomicInteger = new AtomicInteger(0)
  val isPackageSent: AtomicBoolean = new AtomicBoolean(false)
  private val registeredTaskAttempts = new ConcurrentHashMap[Int, ShuffledPartitionInfo]
  def setNextHandler(name: String, next: MessageHandler[Message]): Unit ={
    name match {
      case _ => logger.error("Shuffler do not send message to other components")
    }
  }

  def handleMessage: Message => Unit ={
    case ShufflingPartitionInfo(address,port,split) => {
      logger.info("Shuffler receives ShufflingPartitionInfo")
      val id: Int = currentID.incrementAndGet()
      startPartitionSender(address, port, ShuffledPartitionInfo(id,split))
      isPackageSent.set(false)
      while(!isPackageSent.get()){
        logger.debug( s"Waiting for ShuffledPartitionAck $id")
        Thread.sleep(1000)
      }
    }
    case ShuffledPartitionAck(id) => {
      if(currentID.get() == id) isPackageSent.set(true)
      val currentNumOfSentPartition: Int = numOfSentPartitions.incrementAndGet()
      logger.info(s"Partition with id $id is sent successfully - sent $currentNumOfSentPartition / $totalNumOfSplitNeedToSendOut")
      if(currentNumOfSentPartition == totalNumOfSplitNeedToSendOut){
        logger.info("Shuffler has sent all Partitions")
        slaveController.setAllPartitionSentAndReceive
      }
    }
    case ShuffledPartitionFailed(address, port, message) => {
      logger.info("Sending Partition Failed, try to re-send")
      startPartitionSender(address, port, message)
    }
    case _ => logger.info("Shuffler receives an unexpected Message")
  }

  def startPartitionSender(address: String, port: Int, message: Message): Unit = {
    message match {
      case ShuffledPartitionInfo(id, split) =>{
        logger.info(s"Send Partition with ID $id to $address")
        val partitionSender: PartitionSender = new PartitionSender(this, message, address, port)
/*        val threadPartitionSender = new Thread(partitionSender)
        threadPartitionSender.start()*/
        executor.submit(partitionSender)
      }
      case _ => logger.error("Can only send ShuffledPartitionInfo")
    }
  }

  override def shutdown(): Unit = {
    logger.info("Shuffler shutdown executor and itself")
    Utils.shutdownAndAwaitTermination(executor)
  }
}
