package core

import java.io.File
import java.net.InetAddress
import java.nio.channels.FileChannel
import java.nio.file.Paths

import common.Utils
import core.Message.ShufflingPartitionInfo

/**
  * Created by hminle on 11/2/2016.
  */
object Slave4 extends App{
  override def main(args: Array[String]) {
    val HOST_ADDRESS: String = InetAddress.getLocalHost.getHostAddress
    val slaveController = SlaveController(HOST_ADDRESS, 8511, List.empty, "")
    /*slaveController run()
    logger.info("Slave2 is in state "+ slaveController.getState())*/
    val classLoader = getClass().getClassLoader
    //val file: File = new File(classLoader.getResource("input0.data").getFile)
    val file: File = new File("./input0.data")
    val fileChannel = FileChannel.open(Paths.get(file.getPath))
    val keyValues: List[(Key, Value)] = Utils.getChunkKeyAndValueBySize(10000, fileChannel)._1

    val shuffler = new Shuffler(slaveController, 1)
    shuffler ! ShufflingPartitionInfo(HOST_ADDRESS, 8899, keyValues)
    shuffler ! ShufflingPartitionInfo(HOST_ADDRESS, 8899, keyValues)
    shuffler ! ShufflingPartitionInfo(HOST_ADDRESS, 8899, keyValues)
    shuffler ! ShufflingPartitionInfo(HOST_ADDRESS, 8899, keyValues)
  }
}
