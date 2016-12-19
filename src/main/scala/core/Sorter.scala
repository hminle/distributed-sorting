package core


import com.typesafe.scalalogging.Logger
import core.Message.{ MergingPartitionInfo, SortedMergingPartitionInfo}
import org.slf4j.LoggerFactory

/**
  * Created by hminle on 11/9/2016.
  */
object Sorter {
  def apply(): Sorter =
    new Sorter()
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}

class Sorter()
  extends MessageHandler[Message] {
  import Sorter.logger
  var fileWriter: MessageHandler[Message] = _
  def setNextHandler(name: String, next: MessageHandler[Message]): Unit = {
    name match {
      case "FileWriter" => fileWriter = next
      case _ => logger.error("Sorter can only send message to Partitioner")
    }
  }
  def handleMessage: Message => Unit = {
    case MergingPartitionInfo(fileName, split) => {
      logger.info("Sorter receive MergingPartitionInfo")
      fileWriter ! getSortedChunk(fileName, split)
    }
    case _ => logger.error("Sorter receive an unexpected Message")
  }

  def getSortedChunk(fileName: String, oneChunk: List[(Key, Value)]): Message = {
    val sortedChunk = oneChunk.sortWith(_._1 < _._1)
    SortedMergingPartitionInfo(fileName, sortedChunk)
  }
}
