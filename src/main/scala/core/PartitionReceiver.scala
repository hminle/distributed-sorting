package core

import java.util.concurrent.atomic.AtomicBoolean

import common.ChannelSever
import core.Message.{ShuffledPartitionAck, ShuffledPartitionInfo}

/**
  * Created by hminle on 11/18/2016.
  */
class PartitionReceiver(partitionManager: PartitionManager,
                        hostAddress: String,
                        port: Int)
  extends ChannelSever(hostAddress, port) {
  val isEnoughSplits: AtomicBoolean = new AtomicBoolean(false)
  def name: String = this.getClass.getSimpleName
  def handleRequest(request: Message): Message = {
    request match {
      case ShuffledPartitionInfo(id, split) => {
        partitionManager ! request
        ShuffledPartitionAck(id)
      }
    }
  }
  def checkConditionToStopServer(): Unit = {
    if(isEnoughSplits.get()){
      Thread.currentThread().interrupt()
    }
  }
  def receiveEnoughSplits(): Unit = {
    isEnoughSplits.set(true)
  }
}
