package core

import java.io.File
import java.nio.channels.FileChannel

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import common.Utils
/**
  * Created by hminle on 11/9/2016.
  */
case class Key(key: List[Byte]) extends Ordered[Key] {
  def isEmpty(): Boolean = key.isEmpty
  def compare(that: Key): Int = {
    // For Binary inputs, we need to convert signed bytes into unsigned bytes
    // For ASCII inputs, keys will be the same
    val keys1: List[Int] = this.key.map(_ & 0xff)
    val keys2: List[Int] = that.key.map(_ & 0xff)
    compareTwoKeys(keys1, keys2)
  }
  private def compareTwoKeys(keys1: List[Int], keys2: List[Int]): Int = {
    (keys1, keys2) match {
      case (Nil, Nil) => 0
      case (list, Nil) => 1
      case (Nil, list) => -1
      case (hd1::tl1, hd2::tl2) => {
        if(hd1 > hd2) 1
        else if(hd1 < hd2) -1
        else compareTwoKeys(tl1, tl2)
      }
    }
  }
}
case class Value(value: List[Byte])

object FileSplitter {
  def apply(inputFiles: List[File], chunkSize: Int): FileSplitter =
    new FileSplitter(inputFiles, chunkSize)
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}

// TODO check if file exist
class FileSplitter(inputFiles: List[File], chunkSize: Int) {
  import Message._
  import FileSplitter.logger

  val MINIMUM_FREE_MEMORY = 400000

  var fileChannels: List[(FileChannel, Boolean)] = inputFiles map{
      inputFile => (Utils.getFileChannelFromInput(inputFile), false)}
  var partitioner: MessageHandler[Message] = _

  def setNextHandler(name: String, next: MessageHandler[Message]): Unit ={
    name match {
      case "Partitioner" => partitioner = next
      case _ => logger.error("FileSplitter can only send message to Partitioner")
    }
  }
  def run(): Unit = {
    assert(partitioner != null)
    logger.info("Total input files:" + inputFiles.length)
    var curChunk: Int = 0
    val totalChunk: Int = Utils.getTotalNumOfSplit(inputFiles, chunkSize)

    while(!fileChannels.isEmpty){
      logger.info("FileSplitter is running")
      if(Utils.estimateAvailableMemory() > MINIMUM_FREE_MEMORY){
        val fileChannel = fileChannels(0)._1
        val (chunks, isEndOfFileChannel) = Utils.getChunkKeyAndValueBySize(chunkSize, fileChannel)
        curChunk += 1
        logger.debug( s" FileSplitter get ${chunks.length} keyVal  -- $curChunk / $totalChunk")
        if(isEndOfFileChannel){
          logger.debug( " FileSplitter reaches the end of one input")
          fileChannel.close()
          fileChannels = fileChannels.drop(1)
        }
        logger.info("FileSplitter sends ChunkInfo to Partitioner")
        partitioner ! ChunkInfo(chunks)
      } else {
        logger.info("There is not enough available free memory to continue processing")
      }
    }
    logger.debug( " FileSplitter's finished")
  }
}
