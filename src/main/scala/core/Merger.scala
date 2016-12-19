package core

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.channels.FileChannel

import com.typesafe.scalalogging.Logger

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import common.Utils
import org.slf4j.LoggerFactory
/**
  * Created by hminle on 12/7/2016.
  */
object Merger{
  def apply(outputDir: String, tempDir: String): Merger = new Merger(outputDir, tempDir)
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}
class Merger(outputDir: String, tempDir: String) {
  import Merger.logger
  val MAX_OPEN_FILES = 1000
  def run(): Unit = {
    logger.info( "Merger start running")
    val tempFiles: List[File] = Utils.getListOfFiles(tempDir)
    merge(0, tempDir, outputDir, tempFiles)
  }

  private def merge_aux(outputName: String, tempFiles: List[File]): File = {
    val tempFileChannels: List[FileChannel] = tempFiles.map(Utils.getFileChannelFromInput(_))
    val binaryFileBuffers: ListBuffer[BinaryFileBuffer] = ListBuffer.empty
    tempFileChannels
      .map(BinaryFileBuffer(_))
      .filter(!_.isEmpty())
      .foreach(binaryFileBuffers.append(_))

    val bos = new BufferedOutputStream(new FileOutputStream(outputName))

    while(binaryFileBuffers.length > 0){
      val buffer: BinaryFileBuffer =
        binaryFileBuffers
        .toList
        .sortWith(_.head()._1 < _.head()._1)
        .head
      val keyVal: (Key, Value) = buffer.pop()
      val byteArray: Array[Byte] = Utils.flattenKeyValue(keyVal).toArray[Byte]
      Stream.continually(bos.write(byteArray))
      if(buffer.isEmpty()){
        buffer.close()
        binaryFileBuffers -= buffer
      }
    }
    bos.close()
    new File(outputName)
  }

  @tailrec private def merge(counterTempFile: Int,
                             tempDir: String,
                             outputDir: String,
                             tempFiles: List[File]): File = {
    if(tempFiles.length <= MAX_OPEN_FILES) {
      val outputName: String = outputDir + "/mergedOutput"
      println(Thread.currentThread().getName+ "Writing mergedOutput")
      merge_aux(outputName, tempFiles)
    } else{
      val listTempFileNeedToMerge: List[File] = tempFiles.take(MAX_OPEN_FILES)
      val outputTemp: String = tempDir + "/outputTemp-" + counterTempFile
      val mergedOutputTemp: File = merge_aux(outputTemp, listTempFileNeedToMerge)
      merge(counterTempFile+1, tempDir, outputDir, mergedOutputTemp::tempFiles.drop(MAX_OPEN_FILES))
    }
  }
}


