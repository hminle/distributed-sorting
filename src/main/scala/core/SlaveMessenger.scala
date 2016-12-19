package core

import common.ChannelClient

/**
  * Created by hminle on 10/27/2016.
  */

class SlaveMessenger(message: Message,
                     slaveController: SlaveController,
                     serverAddress: String,
                     serverPort: Int)
  extends ChannelClient(message, serverAddress, serverPort) {
  import core.Message._

  def name: String = this.getClass.getSimpleName

  def handleResponse(response: Message): Unit = {
    slaveController.handleResponse(response)
  }

  def handleFailedMessage(): Unit = {
    slaveController.handleResponse(RequestFail())
  }
}

