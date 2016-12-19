package core

/**
  * Created by hminle on 10/27/2016.
  */
import upickle.default._
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.util

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import Message._
import common.ChannelSever

class MasterMessenger(masterController: MasterController,
                      hostAddress: String,
                      port: Int)
  extends ChannelSever(hostAddress, port) {

  def name: String = this.getClass.getSimpleName

  def handleRequest(request: Message): Message = {
    masterController.handleRequest(request)
  }

  def checkConditionToStopServer(): Unit = {
    if(masterController.getState() == MasterState.Succeed || masterController.getState() == MasterState.Failed){
      masterController stop()
      Thread.currentThread().interrupt()
    }
  }
}
