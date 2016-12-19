package core

import java.io.File
import java.net.InetAddress
import java.util.concurrent.{ExecutorService, Executors}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.typesafe.scalalogging.Logger
import common.Utils
import org.slf4j.LoggerFactory
import Message._
/**
  * Created by hminle on 10/27/2016.
  */
object SlaveController {
  def apply(masterAddress: String,
            masterPort: Int,
            inputDirs: List[String],
            outputDir: String): SlaveController
  = new SlaveController(masterAddress, masterPort, inputDirs, outputDir)
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}

class SlaveController( masterAddress: String,
                          masterPort: Int,
                          inputDirs: List[String],
                          outputDir: String)
  {
  import SlaveController.logger
  private val NUM_OF_THREADS: Int = 1
  private val executor: ExecutorService = Executors.newFixedThreadPool(NUM_OF_THREADS)
  private val SLAVE_ADDRESS: String = InetAddress.getLocalHost.getHostAddress
  private val SLAVE_PORT: Int = 8877
  private var state: SlaveState.State = SlaveState.Initializing
  private val CHUNK_SIZE: Int = 10000
  private var allSlaveKeyRange: List[SlaveKeyRange] = List.empty
  val tempDir: String = outputDir + "temp/"
  val isConfirmationArrived: AtomicBoolean = new AtomicBoolean(false)
  val isPartitionTableArrived: AtomicBoolean = new AtomicBoolean(false)

  val isAllPartitionSentAndReceived: AtomicInteger = new AtomicInteger(0)
  val isFinishConfirmationArrived: AtomicBoolean = new AtomicBoolean(false)
  val inputFiles: List[File] =
    inputDirs.map{fileName => Utils.getFileFromName(fileName)}
  private val totalNumOfSplit: Int = Utils.getTotalNumOfSplit(inputFiles, CHUNK_SIZE)
  logger.info(s"TotalNumOfSplit of mine $totalNumOfSplit")
  private var components: List[MessageHandler[Message]] = List.empty
  var fileSplitter: FileSplitter = _
  var sorter: Sorter = _
  var partitioner: Partitioner = _
  var partitionManager: PartitionManager = _
  var shuffler: Shuffler = _
  var fileWriter: FileWriter = _
  val sorter2: Sorter = Sorter()
  var merger: Merger = _

  def initComponents(): Unit = {
    logger.info("SlaveController inits all components")
    fileSplitter = new FileSplitter(inputFiles, CHUNK_SIZE)
    sorter = new Sorter() with SyncMessageHandler[Message]
    partitioner = new Partitioner() with SyncMessageHandler[Message]
    partitionManager = new PartitionManager(this, outputDir,
      SLAVE_ADDRESS, SLAVE_PORT, tempDir) with SyncMessageHandler[Message]
    val totalNumOfSplitNeedToSendOut: Int = (allSlaveKeyRange.length - 1)*totalNumOfSplit
    logger.info(s"total num of split need to send out $totalNumOfSplitNeedToSendOut")
    shuffler = new Shuffler(this, totalNumOfSplitNeedToSendOut) with SyncMessageHandler[Message]
    fileWriter = new FileWriter() with AsyncMessageHandler[Message]
    merger = new Merger(outputDir, tempDir)
    components = List(sorter, partitioner, partitionManager, shuffler, fileWriter)

    fileSplitter.setNextHandler("Partitioner" ,partitioner)
    partitioner.setNextHandler("PartitionManager" ,partitionManager)
    partitionManager.setNextHandler("Sorter" ,sorter)
    partitionManager.setNextHandler("Shuffler", shuffler)
    sorter.setNextHandler("FileWriter" ,fileWriter)

  }

  def run(): Unit = {
    if(inputFiles.isEmpty){
      logger.info("There is no input to read. Stop Slave now. ")
      Thread.currentThread().interrupt()
    } else {
      logger.info("SlaveController start to running")
      createTempDir(tempDir)
      val sampleData: List[Key] = Utils.getSampleData(inputFiles(0))
      val registerRequest =
        RegisterRequest(totalNumOfSplit, SLAVE_ADDRESS, SLAVE_PORT, sampleData)
      sendMessageToMaster(registerRequest)
      checkArrivedMessageFromMaster(isConfirmationArrived)
      sendMessageToMaster(PartitionTableRequest())
      checkArrivedMessageFromMaster(isPartitionTableArrived)
      initComponents()
      partitioner.setSlaveKeyRange(allSlaveKeyRange)
      partitionManager.setTotalNumOfSplitOfOtherSlaves(getNumOfSplitOfOtherSlaves(SLAVE_ADDRESS))
      fileSplitter.run()
      while(isAllPartitionSentAndReceived.get != 2){
        logger.debug( s" SlaveController waiting for Shuffling --> $isAllPartitionSentAndReceived")
        Thread.sleep(1000)
      }
      Thread.sleep(2000) // wait a while
      merger.run()
      sendMessageToMaster(FinishNotification())
      checkArrivedMessageFromMaster(isFinishConfirmationArrived)
      this.stop()
    }
  }

  def checkArrivedMessageFromMaster(isMessageArrived: AtomicBoolean): Unit ={
    while(!isMessageArrived.get()){
      Thread.sleep(1000)
    }
  }

  def getState(): SlaveState.State = state
  def transitionTo(destinationState: SlaveState.State): Unit = {
    logger.info("Slave from state: " + state + " --> " + destinationState)
    state = destinationState
  }

  def handleResponse(response: Message): Unit = {
    logger.info("Slave handleResponse from Master " + response)
    response match {
      case r: Confirmation => handleConfirmation()
      case r: PartitionTableReturn => handlePartitionTableReturn(r)
      case r: FinishConfirmation => handleFinishConfirmation(r)
      case r: RequestFail => handleRequestFail()
      case _ => logger.info("SlaveController receives an unexpected Message")
    }
  }
  private def handleConfirmation(): Unit = {
    logger.debug("Slave received Confirmation")
    isConfirmationArrived.set(true)
    transitionTo(SlaveState.Waiting)
  }
  private def handlePartitionTableReturn(response: PartitionTableReturn): Unit = {
    logger.debug("Slave received PartitionTableReturn")
    isPartitionTableArrived.set(true)
    allSlaveKeyRange = response.allSlaveKeyRange
    transitionTo(SlaveState.Running)
  }
  private def handleFinishConfirmation(r: Message): Unit = {
    logger.debug("Slave received FinishConfirmation")
    isFinishConfirmationArrived.set(true)
    transitionTo(SlaveState.Succeed)
  }

  private def handleRequestFail(): Unit = {
    Thread.sleep(2000) // wait for a while
    state match {
      case SlaveState.Waiting => sendMessageToMaster(PartitionTableRequest())
      case SlaveState.Running => sendMessageToMaster(FinishNotification())
      case state => logger.info("Something's wrong. Slave is in state " + getState())
    }
  }

  def sendMessageToMaster(request: Message): Unit = {
    logger.info("sendMessageToMaster ")
    logger.debug("message: " + request)
    val slaveMessenger: SlaveMessenger = new SlaveMessenger(request, this, masterAddress, masterPort)
    val threadSlaveMessenger = new Thread(slaveMessenger)
    threadSlaveMessenger.start()

  }

  def createTempDir(tempDir: String): Unit = {
    val tempDirFile: File = new File(tempDir)
    val isSuccessful: Boolean = tempDirFile.mkdir()
    if(isSuccessful) logger.info("Create temp dir successfully")
    else logger.error("Fail to create temp dir")
  }

  def stop(): Unit = {
    Utils.shutdownAndAwaitTermination(executor)
    logger.info("I am done sorting")
    components foreach(_.shutdown())
    val currentGroup: ThreadGroup = Thread.currentThread().getThreadGroup
    val noThreads: Int = currentGroup.activeCount()
    val threads: Array[Thread] = new Array[Thread](noThreads)
    currentGroup.enumerate(threads)
    for(i <- (0 until noThreads)){
      try{
        if(!threads(i).isInterrupted) {
          //println(s"Thread No $i = ${threads(i).getName} --> ${threads(i).isInterrupted}")
          threads(i).interrupt()
        }
      } catch {
        case e: NullPointerException =>
        case e: SecurityException =>
      }
    }
  }

  def setAllPartitionSentAndReceive: Unit = {
    isAllPartitionSentAndReceived.getAndIncrement()
  }
  def getNumOfSplitOfOtherSlaves(myAddress: String): Int = {
    allSlaveKeyRange
      .filter(myAddress != _.slaveInfo.address)
      .foldLeft(0){
        (totalSplit, slaveKeyRange) => totalSplit + slaveKeyRange.slaveInfo.slaveNumOfsplits
      }
  }
}
