package core

/**
  * Created by hminle on 11/2/2016.
  */
import java.net.InetAddress

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import Message._
@RunWith(classOf[JUnitRunner])
class SlaveControllerSuite extends FunSuite {
  val HOST_ADDRESS: String = InetAddress.getLocalHost.getHostAddress
  test("SlaveController in Initializing state transition to Waiting when receiving Confirmation") {
    val slaveController = SlaveController(HOST_ADDRESS, 8511, List.empty, "")
    slaveController handleResponse(Confirmation())
    assert(slaveController.getState() === SlaveState.Waiting)
  }
  test("SlaveController transitions from Waiting to Running"){
    val slaveController = SlaveController(HOST_ADDRESS, 8511, List.empty, "")
    slaveController handleResponse(Confirmation())
    slaveController handleResponse(PartitionTableReturn(PartitionTable(3).getAllSlaveKeyRange()))
    assert(slaveController.getState() === SlaveState.Running)
  }
  test("SlaveController transitions from Running to Succeed"){
    val slaveController = SlaveController(HOST_ADDRESS, 8511, List.empty, "")
    slaveController handleResponse(Confirmation())
    slaveController handleResponse(PartitionTableReturn(PartitionTable(3).getAllSlaveKeyRange()))
    slaveController handleResponse(FinishConfirmation())
    assert(slaveController.getState() === SlaveState.Succeed)
  }

}
