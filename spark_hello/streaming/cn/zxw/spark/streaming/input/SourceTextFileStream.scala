package cn.zxw.spark.streaming.input

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf

/**
 * @author zhangxw
 */
object SourceTextFileStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SourceTextFileStream")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    
    val lines = ssc.textFileStream("file:///D:/GitHub/Spark/spark_hello/file/my/")
    
    val words = lines.flatMap { x => x.split("\\s") }
    val pairs = words.map { x => (x,1) }
    val wordcount = pairs.reduceByKey( _+_ )
    
    wordcount.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}