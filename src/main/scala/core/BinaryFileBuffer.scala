package core

import java.nio.channels.FileChannel

import common.Utils

/**
  * Created by hminle on 12/5/2016.
  */

object BinaryFileBuffer{
  def apply(fileChannel: FileChannel): BinaryFileBuffer = {
    val buffer: BinaryFileBuffer = new BinaryFileBuffer(fileChannel)
    buffer.reload()
    buffer
  }
}
class BinaryFileBuffer(fileChannel: FileChannel) extends Ordered[BinaryFileBuffer] {
  private var cache: Option[(Key, Value)] = _

  def isEmpty(): Boolean = cache == None
  def head(): (Key, Value) = cache.get
  def pop(): (Key, Value) = {
    val answer = head()
    reload()
    answer
  }
  def reload(): Unit = {
    this.cache = Utils.get100BytesKeyAndValue(fileChannel)
  }
  def close(): Unit = fileChannel.close()

  def compare(that: BinaryFileBuffer): Int = {
    this.head()._1.compare(that.head()._1)
  }
}
