package cn.zxw.spark.streaming.api

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
 * @author zhangxw
 */
object TransformOpera {
  //nc -lk 9999
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("TransformOpera")
    val ssc = new StreamingContext(sparkConf,Seconds(10))

    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
    
    val words = lines.flatMap { x => x.split("\\s") }
    val pairs = words.map { x => (x,1) }
    val wordCount = pairs.reduceByKey( _+_ )

    val blacklist = ssc.sparkContext.textFile("spark_streaming_1.6.3/data/words.txt",1).map(x => (x,1))

    wordCount.transform(rdd => {
      rdd.join(blacklist).filter(x => x._2._1>1).map(x => (x._1,x._2._1))
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }
}