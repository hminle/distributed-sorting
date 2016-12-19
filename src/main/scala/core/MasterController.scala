package core

import java.net.InetAddress

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import Message._
/**
  * Created by hminle on 10/27/2016.
  */
object MasterController {
  def apply(slaves: Int): MasterController = new MasterControllerImp(slaves)
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}
trait MasterController {
  def initMessenger(): Unit
  def getState(): MasterState.State
  def transitionTo(destinationState: MasterState.State): Unit
  def handleRequest(slaveRequest: Message): Message
  def stop(): Unit
}

class MasterControllerImp(slaves: Int)
  extends MasterController {
  import MasterController.logger

  val numOfSlaves = slaves
  var numOfRegisterRequest: Int = 0
  var numOfPartitionTableRequest: Int = 0
  var numOfFinishNotification: Int = 0
  var numOfTerminationRequest: Int = 0

  private var state: MasterState.State = MasterState.Initializing
  private val partitionTable: PartitionTable = PartitionTable(numOfSlaves)
  private val HOST_ADDRESS: String = InetAddress.getLocalHost.getHostAddress
  private val PORT: Int = 8517

  def initMessenger(): Unit = {
    val server: MasterMessenger = new MasterMessenger(this, HOST_ADDRESS, PORT)
    val threadServer = new Thread(server)
    threadServer.start()
  }

  def getState(): MasterState.State = state
  def transitionTo(destinationState: MasterState.State): Unit = {
    logger.info("Master from state: " + state + " --> " + destinationState)
    state = destinationState
  }

  def handleRequest(slaveRequest: Message): Message = {
    logger.debug("Master handleRequest from Slave " + slaveRequest + " when Master is in " + this.getState())
    slaveRequest match {
      case r: RegisterRequest => handleRegisterRequest(r)
      case r: PartitionTableRequest => handlePartitionTableRequest(r)
      case r: FinishNotification => handleFinishNotification(r)
    }
  }
  private def handleRegisterRequest(request: RegisterRequest): Message = {
    numOfRegisterRequest += 1
    logger.info(s"Master receives RegisterRequest $numOfRegisterRequest times")

    partitionTable registerSlave(request)
    if(numOfRegisterRequest == numOfSlaves) {
      transitionTo(MasterState.Running)
      partitionTable calculateKeyRange()
    }

    Confirmation()
  }
  //TODO refactor
  private def handlePartitionTableRequest(request: PartitionTableRequest): Message = {
    state match {
      case MasterState.Running => {
        numOfPartitionTableRequest += 1
        logger.info(s"Master receives PartitionTableRequest successfully $numOfPartitionTableRequest times")
        if(numOfPartitionTableRequest == numOfSlaves){transitionTo(MasterState.Waiting)}
        PartitionTableReturn(partitionTable.getAllSlaveKeyRange())
      }
      case _ => RequestFail()
    }
  }

  private def handleFinishNotification(request: FinishNotification): Message = {
    state match {
      case MasterState.Waiting => {
        numOfFinishNotification += 1
        logger.info(s"Master receives FinishNotification $numOfFinishNotification times")
        if(numOfFinishNotification == numOfSlaves){
          logger.info("All Slaves have finished their jobs")
          transitionTo(MasterState.Succeed)
        }
        FinishConfirmation()
      }
      case _ => RequestFail() // Running
    }
  }

  def stop(): Unit = {
    logger.info("Print out Slaves' Address")
    val str = partitionTable getAllSlavesAddress()
    logger.info(str)
  }
}