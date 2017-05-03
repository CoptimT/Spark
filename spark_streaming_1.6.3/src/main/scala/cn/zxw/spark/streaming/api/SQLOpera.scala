package cn.zxw.spark.streaming.api

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zhangxw
 */
object SQLOpera {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("SQLOpera")
    val ssc = new StreamingContext(sparkConf,Seconds(10))
    //ssc.checkpoint("checkpoint")

    ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
      .flatMap { x => x.split("\\s") }
      .foreachRDD(rdd => {
          val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
          import sqlContext.implicits._
          rdd.toDF("word").registerTempTable("words")
          val df = sqlContext.sql("select word,count(*) from words group by word")
          df.show()
      })

    ssc.start()
    ssc.awaitTermination()
  }
}