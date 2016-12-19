package core

/**
  * Created by hminle on 10/31/2016.
  */
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.junit.JUnitRunner
import Message._
@RunWith(classOf[JUnitRunner])
class MasterControllerSuite extends FunSuite with BeforeAndAfter{
  var masterController: MasterController = _
  before{
    masterController = MasterController(3)
  }
  test("MasterController in Initializing state") {

    val registerRequest = RegisterRequest(100, "localhost", 1, List.empty)
    assert(masterController.getState() === MasterState.Initializing)
    masterController handleRequest(registerRequest)
    assert(masterController.getState() === MasterState.Initializing)
  }
  test("MasterController transitions from Initializing to Running state") {
    val registerRequest = RegisterRequest(100, "localhost", 1, List.empty)
    for(i <- (0 until 3)) {masterController handleRequest(registerRequest)}
    assert(masterController.getState() === MasterState.Running)
  }
  test("MasterController return RequestFail in Initializing when received PartitionTableRequest") {
    val partitionTableRequest = PartitionTableRequest()
    val response = masterController handleRequest partitionTableRequest
    assert(response === RequestFail())
  }
  test("Master transitions from Running To Waiting"){
    val registerRequest = RegisterRequest(100, "localhost", 1, List.empty)
    for(i <- (0 until 3)){masterController handleRequest(registerRequest)}
    val partitionTableRequest = PartitionTableRequest()
    for(i <- (0 until 3)) { masterController handleRequest(partitionTableRequest)}
    assert(masterController.getState() === MasterState.Waiting)
  }
  test("Master transitions from Waiting to Terminating") {
    val registerRequest = RegisterRequest(100, "localhost", 1, List.empty)
    for(i <- (0 until 3)){masterController handleRequest(registerRequest)}
    val partitionTableRequest = PartitionTableRequest()
    for(i <- (0 until 3)){masterController handleRequest(partitionTableRequest)}
    val finishNotification = FinishNotification()
    for(i <-(0 until 3)){masterController handleRequest(finishNotification)}
    assert(masterController.getState() === MasterState.Succeed)
  }
}
