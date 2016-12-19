package core

import java.io.{BufferedOutputStream, FileOutputStream}

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import common.Utils
/**
  * Created by hminle on 11/15/2016.
  */
object FileWriter{
  def apply(): FileWriter =
    new FileWriter()
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}

class FileWriter()
  extends MessageHandler[Message] {
  import Message._
  import FileWriter.logger
  private var countSize: Int = 0 //For debugging
  def setNextHandler(name: String, next: MessageHandler[Message]): Unit = {
    name match {
      case _ => logger.error("FileWriter do not send message to other components")
    }
  }

  def handleMessage: Message => Unit = {
    case SortedMergingPartitionInfo(dir, split) => {
      logger.info("Writing partition for Merge later into temp dir")
      writePartition(dir, split)
    }
    case _ => logger.info("FileWriter receives an unexpected Message")
  }

  def writePartition(dir: String, keyValue: List[(Key, Value)]): Unit = {
    val byteArray: Array[Byte] = Utils.flattenKeyValueList(keyValue).toArray[Byte]
    val bos = new BufferedOutputStream(new FileOutputStream(dir))
    Stream.continually(bos.write(byteArray))
    bos.close()
    countSize += keyValue.length
    logger.debug( " FileWriter finish writing file --> " + dir + " SIZE " + keyValue.length)
  }


  override def shutdown(): Unit = {
    logger.info(s"FileWriter has writtern $countSize KeyVal")
  }
}
