package common

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Paths
import java.util.concurrent.{ExecutorService, TimeUnit}

import core.{Key, Value}

import scala.collection.mutable.ListBuffer

/**
  * Created by hminle on 11/13/2016.
  */
object Utils {

  def getSampleData(inputFile: File): List[Key] = {
    val fileChannel: FileChannel = getFileChannelFromInput(inputFile)
    assert(fileChannel != null)
    val size = 10000
    val sampleData = getChunkKeyAndValueBySize(size, fileChannel)._1
    fileChannel.close()
    val sampleDataKeys = sampleData.map{kv => kv._1}
    sampleDataKeys
  }
  //TODO check file exist
  def getFileChannelFromInput(file: File): FileChannel = {
    try{
      val fileChannel: FileChannel = FileChannel.open(Paths.get(file.getPath))
      fileChannel
    } catch{
      case e: IOException => {
        e.printStackTrace()
        null
        //getFileChannelFromInput(file)
      }
    }
  }
  def get100BytesKeyAndValue(fileChannel: FileChannel): Option[(Key, Value)] = {
    val size = 100
    val buffer = ByteBuffer.allocate(size)
    buffer.clear()
    val numOfByteRead = fileChannel.read(buffer)
    buffer.flip()
    if(numOfByteRead != -1){
      val data: Array[Byte] = new Array[Byte](numOfByteRead)
      buffer.get(data, 0, numOfByteRead)
      val (key, value) = data.splitAt(10)
      Some(Key(key.toList), Value(value.toList))
    } else {
      None
    }
  }

  def getChunkKeyAndValueBySize(size: Int, fileChannel: FileChannel): (List[(Key, Value)], Boolean) = {
    val oneKeyValueSize = 100
    val countMax = size / oneKeyValueSize
    var isEndOfFileChannel: Boolean = false
    var count = 0
    val chunks: ListBuffer[(Key, Value)] = ListBuffer.empty
    do{
      val keyValue = get100BytesKeyAndValue(fileChannel)
      if(keyValue.isDefined) chunks.append(keyValue.get)
      isEndOfFileChannel = !keyValue.isDefined
      count += 1
    }while(!isEndOfFileChannel && count < countMax)
  (chunks.toList, isEndOfFileChannel)
  }
  def getFreeMemoryAvailable(): Long = {
    Runtime.getRuntime.freeMemory()
  }

  //TODO check file does not exist
  def getFileFromName(fileName: String): File = {
      val file = new File(fileName)
      file
  }
  def getNumOfSplitOneFile(file: File, chunkSize: Int): Int = {
    val fileSize: Long = file.length()
    if(fileSize > chunkSize){
      val numOfSplit: Int = math.ceil(fileSize / chunkSize.asInstanceOf[Double]).asInstanceOf[Int]
      numOfSplit
    } else 1
  }
/*  def getNumOfSplitOneFile(file: File, numOfKeyValue: Int): Int = {
    val fileSize: Long = file.length()
    val totalOfKeyValueInFile: Int = (fileSize/100L).asInstanceOf[Int]
    if(totalOfKeyValueInFile > numOfKeyValue){
      math.ceil(totalOfKeyValueInFile/numOfKeyValue).asInstanceOf[Int]
    } else 1
  }*/

  def getTotalNumOfSplit(files: List[File], chunkSize: Int): Int = {
    files.map(getNumOfSplitOneFile(_, chunkSize))
      .foldLeft(0)(_ + _)
  }

  def flattenKeyValueList(keyValue: List[(Key,Value)]): List[Byte] = {
    keyValue flatten {
      case (Key(keys), Value(values)) => keys:::values
    }
  }

  def flattenKeyValue(keyVal: (Key, Value)): List[Byte] = {
    keyVal._1.key:::keyVal._2.value
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if(d.exists() && d.isDirectory){
      d.listFiles.filter(_.isFile).toList
    } else  List[File]()
  }

  def estimateAvailableMemory(): Long = {
    //System.gc()
    val runtime: Runtime = Runtime.getRuntime
    val allocatedMemory: Long = runtime.totalMemory() - runtime.freeMemory()
    val presFreeMemory: Long = runtime.maxMemory() - allocatedMemory
    presFreeMemory
  }
  def shutdownAndAwaitTermination(pool: ExecutorService): Unit = {
    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
        pool.shutdownNow() // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(60, TimeUnit.SECONDS))
          System.err.println("Pool did not terminate")
      }
    } catch {
      case e: InterruptedException => {
        // (Re-)Cancel if current thread also interrupted
        pool.shutdownNow()
        // Preserve interrupt status
        Thread.currentThread().interrupt()
      }
    }
  }
}
