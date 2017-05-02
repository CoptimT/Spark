package cn.zxw.spark.streaming.api

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zhangxw
 */
object WindowWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("WindowWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
    
    val words = lines.flatMap { x => x.split("\\s") }
    val pairs = words.map { x => (x,1) }
    val wordcount = pairs.reduceByKeyAndWindow((a:Int,b:Int) => a+b,Seconds(20),Seconds(10))
    
    wordcount.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}