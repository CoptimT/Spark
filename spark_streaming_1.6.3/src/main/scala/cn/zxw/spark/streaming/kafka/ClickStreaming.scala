package cn.zxw.spark.streaming.kafka

import cn.zxw.spark.streaming.listener.CleanStreamingListener
import cn.zxw.spark.streaming.util.{KafkaCluster, MyHiveContext}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ClickStreaming {

  /**
    * 启动Job的时侯设定如下参数
    * topics = args(0)         设置kafka的请求主题
    * brokers = args(1)        设置kafka的broker列表，多个以逗号分隔"192.168.37.3:9092"
    * groupId = args(2)        设置kafka的消费者组id
    * interval = args(3).toInt 设置spreaming的处理间隔，以秒为单位
    */

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Params not enough!!!" + args)
      System.exit(1)
    }

    clickStreaming(args)
  }

  /**
    * 实时清洗的方法
    * @return
    */
  def clickStreaming(args: Array[String]) = {

    val conf = new SparkConf()
      //.setMaster("local[2]")
      .setAppName("ClickStreaming")
    //.set("spark.streaming.unpersist", "true")
    //.set("spark-serializer", "org.apahce.spark.serializer.KvyoSerializer")
    val ssc = new StreamingContext(conf, Seconds(args(3).toInt))

    //add a custom StreamingListener
    ssc.addStreamingListener(new CleanStreamingListener)

    // Kafka configurations
    val topics = Set(args(0))
    val brokers = args(1)
    val groupid = args(2)
    val kafkaParams = Map[String, String](
      "group.id" -> groupid,
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )

    //Create a direct stream by offset
    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets_read: Map[TopicAndPartition, Long] = Map()
    val kc = new KafkaCluster(kafkaParams)

    val scalaTopicAndPartitionSet = kc.getPartitions(topics).right.get

    // 首次消费，默认设置为0
    if (kc.getConsumerOffsets(groupid, scalaTopicAndPartitionSet).isLeft) {
      for (topicAndPartition <- scalaTopicAndPartitionSet) {
        fromOffsets_read += (topicAndPartition -> 0L)
      }
    } else {
      val consumerOffsets = kc.getConsumerOffsets(groupid, scalaTopicAndPartitionSet).right.get

      for (topicAndPartition <- scalaTopicAndPartitionSet) {
        val offset: Long = consumerOffsets.get(topicAndPartition).get
        fromOffsets_read += (topicAndPartition -> offset)
      }
    }

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets_read, messageHandler)

    var offsetRanges = Array.empty[OffsetRange]

    val kafkaStreamTemp = kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val events = kafkaStreamTemp.map(line => {
      var data: JSONObject = null
      try{
        data = JSONObject.fromObject(line._2)
        } catch {
        case e: Exception =>
          println(e.printStackTrace())
          data = new JSONObject
      }
      data
    })

    val schemaString = "req,ua,ip,clt,ak,typ,bid,pid"

    // 用模式字符串生成模式对象
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    events.map(x => Row(x.optString("req"), x.optString("ua"), x.optString("ip"), x.optString("clt"),
                        x.optString("ak"), x.optString("typ"), x.optString("bid"), x.optString("pid")))
      .foreachRDD { rowRdd =>

        val hiveContext = MyHiveContext.getOrCreate(rowRdd.context)
        // 将模式应用于RDD对象。
        val df = hiveContext.createDataFrame(rowRdd, schema)

        hiveContext.sql("use zxw")
        // 将DataFrame注册为表
        df.registerTempTable("persist_log_cs")
        hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        hiveContext.sql("insert into table zxw.persist_log_cs partition(day,hour) select *,from_unixtime(unix_timestamp(req),'yyyyMMdd') day,from_unixtime(unix_timestamp(req),'HH') hour from persist_log_cs where req !='' ")

        //回写kafka的offsets
        for (offsets <- offsetRanges) {
          println(s"========${offsets}====消费的offset=============${offsets.topic} ${offsets.partition} ${offsets.fromOffset} ${offsets.untilOffset}")
          val topicAndPartition = TopicAndPartition(args(0), offsets.partition)
          val o = kc.setConsumerOffsets(args(0), Map((topicAndPartition, offsets.untilOffset)))

          if (o.isLeft) {
            println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
          }
        }

      }

    ssc.start()
    ssc.awaitTermination()
  }
}
