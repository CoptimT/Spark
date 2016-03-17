package cn.zxw.spark.streaming.input

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import java.io.InputStream
import java.io.BufferedInputStream

/**
 * @author zhangxw error
 */
object SourceSocketStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SourceSocketStream")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    
    //MySocketServer
    val lines = ssc.socketStream[String]("localhost", 9999, converter, StorageLevel.MEMORY_ONLY)
    
    val words = lines.flatMap { x => x.split("\\s") }
    val pairs = words.map { x => (x,1) }
    val wordcount = pairs.reduceByKey( _+_ )
    
    wordcount.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
  //converter: InputStream => Iterator[T]
  def converter(is:InputStream):Iterator[String] = {
    val bis = new BufferedInputStream(is)
    //bis.readLine().Iterator
    val ite = List("").toIterator
    ite
  }
}