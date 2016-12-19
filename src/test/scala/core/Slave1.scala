package core

import java.io.File
import java.net.InetAddress

import com.typesafe.scalalogging.Logger
import common.Utils
import org.slf4j.LoggerFactory
import Message._
/**
  * Created by hminle on 11/2/2016.
  */
object Slave1 extends App{
  override def main(args: Array[String]) {
    val HOST_ADDRESS: String = "141.223.82.184" //InetAddress.getLocalHost.getHostAddress
    //val slaveController = SlaveController(HOST_ADDRESS, 8511, List.empty, "")
/*    val classLoader: ClassLoader = getClass().getClassLoader
    val file = new File(classLoader.getResource("input0.data").getFile)*/
    val input = "./input0.data"
    val slaveController = DummySlaveController(HOST_ADDRESS, 8517, List(input), "./tmp")
    slaveController initComponents()
    slaveController run()

    println("Slave1 is in state "+ slaveController.getState())
  }
}
object DummySlaveController{
  def apply(address: String, port: Int, inputs: List[String], output: String): DummySlaveController = new DummySlaveController(address, port, inputs, output)
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}
class DummySlaveController(address: String, port: Int, inputs: List[String], output: String)
  extends SlaveController(address, port, inputs, output){
  import DummySlaveController.logger
/*  override val inputFiles: List[File] =
    inputs.map{fileName => Utils.getFileFromName(fileName)}*/
/*  override def run(): Unit = {
    assert(sorter != null)
    if(inputFiles.isEmpty){
      logger.info("There is no input to read. Stop Slave now. ")
      Thread.currentThread().interrupt()
    } else {
      val sampleData: List[Key] = Utils.getSampleData(inputFiles(0))
      val registerRequest =
        RegisterRequest(1, address, port, sampleData)
      //sendMessageToMaster(registerRequest)
      //initServices()
      logger.info(sorter == null)
      fileSplitter.run()
    }
  }*/
  override def sendMessageToMaster(request: Message): Unit = {
    isConfirmationArrived.set(true)
    isPartitionTableArrived.set(true)
  }
}