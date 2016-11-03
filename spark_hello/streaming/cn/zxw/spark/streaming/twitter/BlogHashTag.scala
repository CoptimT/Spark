package cn.zxw.spark.streaming.hello

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils

/**
 * @author zhangxw
 * 1.HashTag是微博内容中的标签,Twitter提供了流式获取HashTag的API
 * 2.程序不完整
 */
object BlogHashTag {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("BlogHashTag")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    
    val twitterStream = TwitterUtils.createStream(ssc, twitterAuth=None)
    val hashTags = twitterStream.map { status => status.getText }.filter(word => word.startsWith("#Spark"))
    //val hashTags = twitterStream.flatMap { status => getTags(status) }
    hashTags.countByValue().print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}