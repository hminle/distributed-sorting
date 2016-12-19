package common

/**
  * Created by hminle on 11/18/2016.
  */
import java.io.{File, FileInputStream}
import java.nio.channels.FileChannel
import java.nio.file.Paths

import core.{Key, Value}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UtilsSuite extends FunSuite {
  test("Check total number of split"){
    val classLoader = getClass().getClassLoader
    val file: File = new File(classLoader.getResource("input0.data").getFile)
    val totalSplit: Int = Utils.getTotalNumOfSplit(List(file), 10000)
    assert(totalSplit === 1049)
  }

  test("Check number of times need to split a file"){
    val classLoader = getClass().getClassLoader
    val file: File = new File(classLoader.getResource("input0.data").getFile)
    val fileChannel = FileChannel.open(Paths.get(file.getPath))
    var count = 0
    //var chunks: List[(Key, Value)] = List.empty
    var isEndOfFileTemp: Boolean = false
    do {
      val (chunks, isEndOfFile) = Utils.getChunkKeyAndValueBySize(10000, fileChannel)
      count += 1
      isEndOfFileTemp = isEndOfFile
    }while(!isEndOfFileTemp)
    val totalSplit: Int = Utils.getTotalNumOfSplit(List(file), 10000)
    assert(count === 1049)
  }
}
