package core

import java.net.InetAddress

/**
  * Created by hminle on 11/2/2016.
  */
object Master {
  def main(args: Array[String]) {
    val masterAddress: String = InetAddress.getLocalHost.getHostAddress
    println(masterAddress+":"+"8517")
    val numOfSlaves = args(0).toInt
    val masterController = MasterController(numOfSlaves)
    masterController initMessenger()
  }
}
