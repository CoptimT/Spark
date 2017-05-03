package cn.zxw.spark.streaming.api

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zhangxw
 */
object JoinOpera {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("JoinOpera")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //ssc.checkpoint("checkpoint")

    //1.Stream-stream joins
    val wc1 = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY).flatMap { x => x.split("\\s") }.map((_,1)).reduceByKey(_+_)
    //val wc2 = ssc.socketTextStream("localhost", 9998, StorageLevel.MEMORY_ONLY).flatMap { x => x.split("\\s") }.map((_,1)).reduceByKey(_+_)

    //wc1.join(wc2).print()
    //wc1.leftOuterJoin(wc2).print()
    //wc1.rightOuterJoin(wc2).print()
    //wc1.fullOuterJoin(wc2).print()

    //2.Stream-dataset joins
    wc1.transform(rdd => {
      val blist = ssc.sparkContext.textFile("spark_streaming_1.6.3/data/words.txt").map((_,1))//real-time update
      rdd.join(blist)
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }
}