package core

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by hminle on 11/9/2016.
  */
trait MessageHandler[T]{

  def setNextHandler(name: String, next: MessageHandler[T]): Unit
  def handleMessage: T => Unit

  def !(message: T) = handleMessage(message)
  def shutdown(): Unit = {}
}

trait AsyncMessageHandler[T] extends MessageHandler[T] {
  val handlerContext =
    ExecutionContext.fromExecutorService(java.util.concurrent.Executors.newFixedThreadPool(1))

  override def !(message: T) = Future {handleMessage(message)} (handlerContext)
  abstract override def handleMessage: T => Unit = super.handleMessage
  override def shutdown(): Unit = {
    handlerContext.shutdown()
    super.shutdown()
  }
}

trait SyncMessageHandler[T] extends MessageHandler[T] {
  abstract override def handleMessage: T => Unit = super.handleMessage
  override def shutdown(): Unit = {
    super.shutdown()
  }
}
