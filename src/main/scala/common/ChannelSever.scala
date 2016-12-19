package common

/**
  * Created by hminle on 11/18/2016.
  */

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.util

import com.typesafe.scalalogging.Logger
import core.Message
import org.slf4j.LoggerFactory

object ChannelSever {
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))
}
abstract class ChannelSever(hostAddress: String, port: Int)
  extends Runnable {
  import ChannelSever.logger

  private val TIMEOUT: Long = 10000
  private var serverChannel: ServerSocketChannel = _
  private var selector: Selector = _

  def checkConditionToStopServer(): Unit
  def name: String
  def handleRequest(request: Message): Message

  val dataTracking: util.HashMap[SocketChannel, Array[Byte]] = new util.HashMap[SocketChannel, Array[Byte]]()
  def init(): Unit = {
    logger.info(s"Initalizing $name Server")
    //assert(selector != null)
    //assert(serverChannel != null)
    try {
      // Open a Selector
      selector = Selector.open()
      // Open a ServerSocketChannel
      serverChannel = ServerSocketChannel.open()
      // Configure for non-blocking
      serverChannel.configureBlocking(false)
      // bind to the address
      serverChannel.socket().bind(new InetSocketAddress(hostAddress, port))

      // Told selector that this channel will be used to accept connections
      // We can change this operation later to read/write
      serverChannel.register(selector, SelectionKey.OP_ACCEPT)
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }

  override def run(): Unit = {
    init()
    logger.info("Ready to accept connections from Senders....")

    try {
      while(!Thread.currentThread().isInterrupted){
        logger.debug(s"$name Server is running")
        //checkMasterControllerState()
        // blocking call, can use TIMEOUT here
        selector.select()

        val keys: util.Iterator[SelectionKey] = selector.selectedKeys().iterator()
        while(keys.hasNext) {
          val key: SelectionKey = keys.next()

          //remove the key so that we don't process this OPERATION again
          keys.remove()

          //key could be invalid if for example the client closed the connection
          assert(key.isValid)
          if(key.isAcceptable){
            logger.debug("Accepting connections...")
            accept(key)
          }
          if(key.isWritable){
            logger.debug("Writing...")
            write(key)
          }
          if(key.isReadable){
            logger.debug("Reading connections....")
            read(key)
          }
        }
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      closeConnection()
    }
  }

  private def accept(key: SelectionKey): Unit = {
    val serverSocketChannel: ServerSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    socketChannel.configureBlocking(false)
    //TODO Check wakeup
    //selector.wakeup()
    socketChannel.register(selector, SelectionKey.OP_READ)
  }

  private def write(key: SelectionKey): Unit = {
    val channel: SocketChannel = key.channel().asInstanceOf[SocketChannel]
    val data: Array[Byte] = dataTracking.get(channel)
    channel.write(ByteBuffer.wrap(data))

    key.interestOps(SelectionKey.OP_READ)
    checkConditionToStopServer()
  }
  private def read(key: SelectionKey): Unit = {
    val channel: SocketChannel = key.channel().asInstanceOf[SocketChannel]
    val readBuffer: ByteBuffer = ByteBuffer.allocate(3000000)
    readBuffer.clear()
    var length: Int = 0
    try {
      length = channel.read(readBuffer)
      if(length == -1){
        logger.debug("Nothing was there to be read, closing connection")
        channel.close()
        key.cancel()
      } else {
        readBuffer.flip()
        val data: Array[Byte] = new Array[Byte](length)
        readBuffer.get(data, 0 ,length)

        val receivedRequestStr: String = new String(data)
        val receivedRequest: Message = upickle.default.read[Message](receivedRequestStr)
        val responseRequest: Message = handleRequest(receivedRequest)
        val responseRequestStr: String = upickle.default.write(responseRequest)
        respond(key, responseRequestStr.getBytes)
      }
    } catch {
      case e: IOException => {
        logger.debug("Reading problem, closing connection")
        key.cancel()
        channel.close()
        return
      }
    }
  }
  private def respond(key: SelectionKey, data: Array[Byte]): Unit = {
    val socketChannel = key.channel().asInstanceOf[SocketChannel]
    dataTracking.put(socketChannel, data)
    key.interestOps(SelectionKey.OP_WRITE)
  }

  private def closeConnection(): Unit = {
    logger.info(s"Closing $name Server down")
    if(selector != null){
      try{
        selector.close()
        serverChannel.socket().close()
        serverChannel.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }
}
