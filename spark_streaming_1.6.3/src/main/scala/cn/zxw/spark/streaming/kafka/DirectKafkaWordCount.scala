package cn.zxw.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object DirectKafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.exit(1)
    }
    val Array(masterUrl, brokers, topics) = args

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("DirectKafkaStream1")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    
    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split("\\s"))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}
