package cn.zxw.spark.streaming.hello

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

/**
 * @author zhangxw
 */
object StatefulWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    ssc.checkpoint("D:\\GitHub\\Spark\\spark_hello\\file\\checkpoint\\")
    
    //MySocketServer
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY_SER_2)
    
    val words = lines.flatMap { x => x.split("\\s") }
    val pairs = words.map { x => (x,1) }
    val wordcount = pairs.reduceByKey( _+_ )
    
    val stateDStream = wordcount.updateStateByKey[Int](updateFunc)
    stateDStream.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
  
  def updateFunc = (values:Seq[Int], state:Option[Int]) => {
    val currCount = values.foldLeft(0)(_+_)
    val statCount = state.getOrElse(0)
    Some(currCount + statCount)
  }
}