package cn.zxw.spark.streaming.hello

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

/**
 * @author zhangxw
 */
object WindowTransformation {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WindowTransformation")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    
    //MySocketServer
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
    val words = lines.flatMap { x => x.split("\\s") }
    
    words.window(Seconds(10),Seconds(5)).countByValue().print()//DStream[(String, Long)]
    words.countByWindow(Seconds(10),Seconds(5)).print()//DStream[Long]
    words.countByValueAndWindow(Seconds(10), Seconds(5)).print()//DStream[(String, Long)]
    
    words.reduceByWindow((a:String,b:String) => a+","+b, Seconds(10), Seconds(5))//DStream[T]
    val wc = words.map((_,1))
    wc.reduceByKeyAndWindow((a:Int,b:Int) => a+b,Seconds(20),Seconds(10))//DStream[(K, V)]
    wc.reduceByKeyAndWindow((a:Int,b:Int) => a+b,(a:Int,b:Int) => a+b,Seconds(20),Seconds(10))//DStream[(K, V)]
    
    wc.updateStateByKey((values:Seq[Int],state:Option[Int]) => Some(values.sum + state.getOrElse(0)))
    
    ssc.start()
    ssc.awaitTermination()
  }
}