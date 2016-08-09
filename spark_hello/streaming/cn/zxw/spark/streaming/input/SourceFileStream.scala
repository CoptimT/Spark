package cn.zxw.spark.streaming.input

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.Text

/**
 * @author zhangxw
 */
object SourceFileStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SourceFileStream")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    
    val directory = "file:///Users/zhangxw/Desktop/log/temp/"
    val inputDStream = ssc.fileStream[LongWritable,Text,TextInputFormat](directory)
    
    val lines = inputDStream.map(kv => kv._2.toString())
    val words = lines.flatMap { x => x.split("\\s") }.map { x => (x,1) }
    val wordcount = words.reduceByKey( _+_ )
    
    wordcount.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}