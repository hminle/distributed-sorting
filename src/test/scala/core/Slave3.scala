package core

import java.net.InetAddress

/**
  * Created by hminle on 11/2/2016.
  */
object Slave3 extends App{
  override def main(args: Array[String]) {
    val HOST_ADDRESS: String = InetAddress.getLocalHost.getHostAddress
    val slaveController = SlaveController(HOST_ADDRESS, 8511, List.empty, "")
/*    slaveController run()
    logger.info("Slave3 is in state "+ slaveController.getState())*/
    val partitionManager = new PartitionManager(slaveController,"./", HOST_ADDRESS, 8899, "")
    partitionManager.setTotalNumOfSplitOfOtherSlaves(8)
    val fileWriter = new FileWriter(){
      override def writePartition(dir: String, keyValues: List[(Key, Value)]): Unit ={
        println("SLAVE 3 RECEIVE PARTITION SUCCESSFULLY")
      }
    }
    partitionManager.setNextHandler("FileWriter", fileWriter)

  }
}
