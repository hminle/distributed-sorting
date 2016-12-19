package core

import java.net.InetAddress

/**
  * Created by hminle on 11/2/2016.
  */
object Slave{
  def main(args: Array[String]) {
    val(masterAddress, masterPort) = getMasterAddressAndPort(args(0))
    val slaveController: SlaveController = SlaveController(masterAddress, masterPort, args.drop(1).dropRight(1).toList, args.last)

    slaveController run()
    println("Slave is in state "+ slaveController.getState())
    println(s"Output dir: ${args.last}mergedOutput")
  }
  private def getMasterAddressAndPort(hostName: String): (String, Int)= {
    val hostNameArray = hostName.split(":")
    (hostNameArray(0), hostNameArray(1).toInt)
  }
}
