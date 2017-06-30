package cn.zxw.spark.yixia

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import kafka.client.ClientUtils
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.SparkSession

object GetOffsetByTimeStamp {

  case class OffsetInfo2(topic: String, partition: Int, offsetBegin: Long, offsetEnd: Long)

  def main(args: Array[String]) {
    val topics = args(0)
    val brokerList = args(1)
    val clientId = "GetOffsetByTimeStamp"
    val maxWaitMs = 1000
    val path = args(2)

    val date_time_begin = args(3)
    var timestamp_begin = 0l

    if (date_time_begin.length == 13) {
      timestamp_begin = date_time_begin.toLong
    } else if (date_time_begin.length == 12) {
      timestamp_begin = new SimpleDateFormat("yyyyMMddHHmm").parse(date_time_begin).getTime
    }
    println("timestamp_begin=" + timestamp_begin)

    val date_time_end = args(4)
    var timestamp_end = 0l

    if (date_time_end.length == 13) {
      timestamp_end = date_time_end.toLong
    } else if (date_time_end.length == 12) {
      timestamp_end = new SimpleDateFormat("yyyyMMddHHmm").parse(date_time_end).getTime
    }
    println("timestamp_end=" + timestamp_end)

    val spark = SparkSession.builder().appName("kafka:GetMessageByTimeStamps" + topics).getOrCreate()

    val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)
    val topicsMetadata = ClientUtils.fetchTopicMetadata(topics.split(",").toSet, metadataTargetBrokers, clientId, maxWaitMs)
      .topicsMetadata
    val partitions = topicsMetadata.head.partitionsMetadata.map(_.partitionId)

    val props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("group.id", "test_caojian_timestamp");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    val kafkaConsumer = new KafkaConsumer(props)

    var offsetsMap: Map[Int, (Long, Long)] = Map[Int, (Long, Long)]()

    /**
      * timestamp_begin
      */
    topicsMetadata.foreach { topicMetadata =>
      val topic = topicMetadata.topic
      partitions.foreach { partitionId =>
        val partitionMetadataOpt = topicMetadata.partitionsMetadata.find(_.partitionId == partitionId)
        partitionMetadataOpt match {
          case Some(metadata) =>
            metadata.leader match {
              case Some(leader) =>

                /**
                  */
                val topicPartitionBegin = new TopicPartition(topic, partitionId)
                println("topicPartitionBegin=" + topic + "\tpartitionId=" + partitionId)

                val timestampsToSerachMap: util.Map[TopicPartition, java.lang.Long] = new util.HashMap[TopicPartition, java.lang.Long]()

                timestampsToSerachMap.put(topicPartitionBegin, new java.lang.Long(timestamp_begin))

                val offsetAndTimestampMaps: util.Map[TopicPartition, OffsetAndTimestamp] = kafkaConsumer.offsetsForTimes(timestampsToSerachMap)

                if (offsetAndTimestampMaps == null || offsetAndTimestampMaps.size() == 0) {
                  println("offsetAndTimestampMaps is null !!")
                }

                val offsetAndTimestampBegin: OffsetAndTimestamp = offsetAndTimestampMaps.get(topicPartitionBegin)

                if (offsetAndTimestampBegin == null) {
                  println("offsetAndTimestampBegin is null ")
                }

                val offsetsBegin = offsetAndTimestampBegin.offset()
                println("offsetsBegin=" + offsetsBegin + "\t timestamp_begin=" + timestamp_begin)

                offsetsMap += (partitionId -> (offsetsBegin, 0l))

              case None =>
                System.err.println("Error: partition %d does not have a leader. Skip getting offsets".format(partitionId))
            }
          case None => System.err.println("Error: partition %d does not exist".format(partitionId))
        }
      }
    }

    println("offsetsMap.size=" + offsetsMap.size)

    /**
      * timestamp_end
      */
    topicsMetadata.foreach { topicMetadata =>
      val topic = topicMetadata.topic
      partitions.foreach { partitionId =>
        val partitionMetadataOpt = topicMetadata.partitionsMetadata.find(_.partitionId == partitionId)
        partitionMetadataOpt match {
          case Some(metadata) =>
            metadata.leader match {
              case Some(leader) =>

                val topicPartitionEnd = new TopicPartition(topic, partitionId)
                println("topicPartitionEnd=" + topic + "\tpartitionId=" + partitionId)

                val timestampsToSerachMap: util.Map[TopicPartition, java.lang.Long] = new util.HashMap[TopicPartition, java.lang.Long]()

                timestampsToSerachMap.put(topicPartitionEnd, new java.lang.Long(timestamp_end))

                val offsetAndTimestampMaps: util.Map[TopicPartition, OffsetAndTimestamp] = kafkaConsumer.offsetsForTimes(timestampsToSerachMap)

                if (offsetAndTimestampMaps == null || offsetAndTimestampMaps.size() == 0) {
                  println("offsetAndTimestampMaps is null ")
                }

                val offsetAndTimestampEnd: OffsetAndTimestamp = offsetAndTimestampMaps.get(topicPartitionEnd)

                if (offsetAndTimestampEnd == null) {
                  println("offsetAndTimestampEnd is null ")
                }

                val offsetsEnd = offsetAndTimestampEnd.offset()
                println("offsetsEnd=" + offsetsEnd + "\t timestamp_begin=" + timestamp_end)

                var offsetsBegin = 0l
                if (offsetsMap.contains(partitionId)) {
                  offsetsBegin = offsetsMap.get(partitionId).get._1
                  offsetsMap += (partitionId -> (offsetsBegin, offsetsEnd))
                }
              case None =>
                System.err.println("Error: partition %d does not have a leader. Skip getting offsets".format(partitionId))
            }
          case None => System.err.println("Error: partition %d does not exist".format(partitionId))
        }
      }
    }

    var list1: List[OffsetInfo2] = List()
    offsetsMap.foreach(e => {
      val (k, v) = e
      println(k + ":" + v)
      list1 = OffsetInfo2(topics, k, v._1, v._2) :: list1
    })

    // write
    //val ds = list1.toDS()
    //ds.repartition(1).write.parquet(path)
  }
}
