package core

import common.ChannelClient

/**
  * Created by hminle on 11/18/2016.
  */
class PartitionSender(shuffler: Shuffler,
                      message: Message,
                      serverAddress: String,
                      serverPort: Int)
  extends ChannelClient(message, serverAddress, serverPort) {
  import core.Message._
  def name: String = this.getClass.getSimpleName
  def handleResponse(response: Message): Unit = {
    shuffler ! response
  }

  def handleFailedMessage(): Unit ={
    shuffler ! ShuffledPartitionFailed(serverAddress, serverPort, message)
  }
}
