package common

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, SocketChannel}
import java.util

import com.typesafe.scalalogging.Logger
import core.Message
import core.Message.ShuffledPartitionFailed
import org.slf4j.LoggerFactory


/**
  * Created by hminle on 11/18/2016.
  */

object ChannelClient {
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}
abstract class ChannelClient(message: Message,
                             serverAddress: String,
                             serverPort: Int)
  extends Runnable {
  import ChannelClient.logger

  var selector: Selector = _
  def name: String
  def handleResponse(response: Message): Unit
  def handleFailedMessage(): Unit

  override def run(): Unit = {
    try{
      selector = Selector.open()
      val channel = SocketChannel.open()
      channel.configureBlocking(false)
      channel.register(selector, SelectionKey.OP_CONNECT)
      channel.connect(new InetSocketAddress(serverAddress, serverPort))

      while(!Thread.interrupted()) {
        selector.select()
        val keys: util.Iterator[SelectionKey] = selector.selectedKeys().iterator()
        while(keys.hasNext){
          val key = keys.next()
          keys.remove()
          assert(key.isValid)

          if(key.isConnectable){
            logger.info(s"$name am connected to the receiver")
            connect(key)
          }
          if(key.isValid && key.isWritable){
            write(key)
          }
          if(key.isValid && key.isReadable){
            read(key)
          }
        }
      }
    } catch {
      case e: IOException => {
        logger.error("Get IOException when running")
        e.printStackTrace()
      }
    } finally {
      //TODO really need to resend here???
      //handleFailedMessage()
      closeConnection()
    }
  }
  private def closeConnection(): Unit = {
    try {
      selector.close()
    } catch {
      case e: IOException => {
        logger.error("Get IOException when closing")
        e.printStackTrace()
      }
    }
  }

  private def connect(key: SelectionKey): Unit = {
    val channel: SocketChannel = key.channel().asInstanceOf[SocketChannel]
    try{

      if(channel.isConnectionPending){
        channel.finishConnect()
      }
      channel.configureBlocking(false)
      channel.register(selector, SelectionKey.OP_WRITE)
    } catch {
      case e: java.net.ConnectException => {
        e.printStackTrace()
        logger.info( "Try to re-send")
        //handleResponse(ShuffledPartitionFailed(serverAddress, serverPort, message))
        handleFailedMessage()
        key.cancel()
        channel.close()
        Thread.sleep(100)
        closeConnection()
        Thread.currentThread().interrupt()
      }
    }
  }

  private def write(key: SelectionKey): Unit = {
    val channel: SocketChannel = key.channel().asInstanceOf[SocketChannel]
    val messageStr: String = upickle.default.write(message)
    try{
      logger.debug(s"$name is ready to send message with ${messageStr.getBytes.length} bytes")
      channel.write(ByteBuffer.wrap(messageStr.getBytes))
      key.interestOps(SelectionKey.OP_READ)
    } catch {
      case e: IOException => {
        e.printStackTrace()
        handleResponse(ShuffledPartitionFailed(serverAddress, serverPort, message))
        key.cancel()
        channel.close()
        Thread.sleep(100)
        closeConnection()
        Thread.currentThread().interrupt()
      }
    }
  }
  private def read(key: SelectionKey): Unit = {
    val channel: SocketChannel = key.channel().asInstanceOf[SocketChannel]
    val readBuffer: ByteBuffer = ByteBuffer.allocate(3000000)
    readBuffer.clear()
    var length: Int = 0
    try{
      length = channel.read(readBuffer)
      if(length == -1){
        logger.info("Nothing was read from receiver")
        channel.close()
        key.cancel()
        Thread.currentThread().interrupt()
      } else {
        readBuffer.flip()
        val data: Array[Byte] = new Array[Byte](length)
        readBuffer.get(data, 0 , length)
        val responseStr: String = new String(data)
        val response: Message = upickle.default.read[Message](responseStr)
        handleResponse(response)
        logger.debug(s"Shutdown $name sender")
        closeConnection()
        Thread.currentThread().interrupt()
      }
    } catch{
      case e: IOException => {
        logger.debug("Reading in Sender has problem, closing connection")
        key.cancel()
        channel.close()
        closeConnection()
        Thread.currentThread().interrupt()
      }
    }
  }
}
