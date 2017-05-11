package cn.zxw.spark.streaming.kafka

import java.net.InetAddress

import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.zookeeper.CreateMode
import spire.syntax.group

import scala.reflect.ClassTag
import scala.util.control.NonFatal

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


    val dstream: DStream[(String, Array[Byte])] = fromOffsets.map { fo =>
      val messageHandler: MessageAndMetadata[String, Array[Byte]] => (String, Array[Byte]) = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.key, mmd.message)
      KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, Tuple2[String, Array[Byte]]](ssc, kafkaParams, fo, messageHandler)
    } getOrElse KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc = ssc, kafkaParams = kafkaParams, topics = topicsSet)

    dstream.foreachRDDWithOffsets(zkQuorum,name,group,topic){ rdd =>


    }




    /*
    val dstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split("\\s"))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()*/
    
    ssc.start()
    ssc.awaitTermination()
  }

  def getFromOffsets(zk: String, topic: String, group: String, partitions: Int)(implicit logger: org.log4s.Logger): Option[Map[TopicAndPartition, Long]] = {
    val zkClient = new ZkClientImpl(zk, java.util.UUID.randomUUID.toString)

    val list = (0 to partitions - 1).toList.flatMap { i =>
      val nodePath = s"/consumers/$group/offsets/$topic/$i"

      try {
        zkClient.exists(nodePath) match {
          case true =>
            val maybeOffset = Option(zkClient.getRaw(nodePath))
            logger info s"Kafka Direct Stream - From Offset - ZK Node ($nodePath) - Value: $maybeOffset"
            maybeOffset.map { offset =>
              TopicAndPartition(topic, i) -> new String(offset).toLong
            }

          case false =>
            logger info s"Kafka Direct Stream - From Offset - ZK Node ($nodePath) does NOT exist"
            None
        }
      } catch {
        case scala.util.control.NonFatal(error) =>
          logger error s"Kafka Direct Stream - From Offset - ZK Node ($nodePath) - Error: $error"
          None
      }
    }

    (list.size =:= partitions) match {
      case true =>
        list.foreach {
          case (t, offset) =>
            logger info s"From Offset - Topic: ${t.topic}, Partition: ${t.partition}, Offset: ${offset}"
        }

        Some(list.toMap)

      case false =>
        logger info s"Current ZK offsets (${list.size} doesn't matc partition size, so gonna rely on Spark checkpoint to reuse Kafka offsets"
        None
    }
  }

  implicit class PimpDStream[T: ClassTag](dstream: DStream[T]) extends Serializable {

    def foreachRDDWithOffsets(zkQuorum: String, name: String, group: String, topic: String)(f: RDD[T] => Unit)(implicit logger: org.log4s.Logger): Unit = {
      dstream.foreachRDD { rdd =>
        // Connect to ZK
        implicit val zk = new ZkClientImpl(zkQuorum, name)
        try {
          // Try to store offsets in ZK
          try {
            rdd.zkOffsets(group)
          } catch {
            case NonFatal(error) =>
              logger.error("DStream Foreach RDD with Offset - Store Offsets - Error: $error")
          }
          f(rdd) // Do the work
        } catch {
          case NonFatal(error) =>
            logger.error("DStream Foreach RDD with Offset - Error: $error")
            throw error
        } finally {
          // Always cleanup ZK connections
          zk.close()
        }
      }
    }
  }

  implicit class PimpRDD[T: ClassTag](rdd: RDD[T]) {

    def zkOffsets(group: String)(implicit zk: ZkClientImpl, logger: org.log4s.Logger): Unit = {
      rdd match {
        case offsetRdd: HasOffsetRanges =>
          val offsets = offsetRdd.offsetRanges
          offsets.foreach { o =>
            // Consumer Offset
            locally {
              val nodePath = s"/consumers/$group/offsets/${o.topic}/${o.partition}"

              logger.info(s"Kafka Direct Stream - Offset Range - Topic: ${o.topic}, Partition: ${o.partition}, From Offset: ${o.fromOffset}, To Offset: ${o.untilOffset}, ZK Node: $nodePath")

              zk.exists(nodePath) match {
                case true =>
                  logger info s"Kafka Direct Stream - Offset - ZK Node ($nodePath) exists, setting value: ${o.untilOffset}"
                  zk.setRaw(nodePath, o.untilOffset.toString.getBytes)

                case false =>
                  logger info s"Kafka Direct Stream - Offset - ZK Node ($nodePath) does NOT exist -- setting value: ${o.untilOffset}"
                  zk.createRaw(nodePath, o.untilOffset.toString.getBytes(), createMode = Some(CreateMode.PERSISTENT))
              }
            }

            val hostname = InetAddress.getLocalHost().getHostName()
            val ownerId = s"${group}-${hostname}-${o.partition}"
            val now = org.joda.time.DateTime.now.getMillis

            // Consumer Ids
            locally {
              val nodePath = s"/consumers/$group/ids/${ownerId}"
              val value = s"""{"version":1,"subscription":{"${o.topic}":${o.partition},"pattern":"white_list","timestamp":"$now"}"""

              zk.exists(nodePath) match {
                case true =>
                  logger info s"Kafka Direct Stream - Id - ZK Node ($nodePath) exists, setting value: ${value}"
                  zk.setRaw(nodePath, value.getBytes)

                case false =>
                  logger info s"Kafka Direct Stream - Id - ZK Node ($nodePath) does NOT exist -- setting value: ${value}"
                  zk.createRaw(nodePath, value.getBytes, createMode = Some(CreateMode.PERSISTENT))
              }
            }

            // Consumer Owners
            locally {
              val nodePath = s"/consumers/$group/owners/${o.topic}/${o.partition}"
              val value = ownerId

              zk.exists(nodePath) match {
                case true =>
                  logger info s"Kafka Direct Stream - Owner - ZK Node ($nodePath) exists, setting value: ${value}"
                  zk.setRaw(nodePath, value.getBytes)

                case false =>
                  logger info s"Kafka Direct Stream - Owner - ZK Node ($nodePath) does NOT exist -- setting value: ${value}"
                  zk.createRaw(nodePath, value.getBytes, createMode = Some(CreateMode.PERSISTENT))
              }
            }
          }

        case _ =>
          logger warn s"DStream - ZK Offsets - Cannot store on ZK since RDD is not of type of HasOffsetRanges: $rdd"
      }
    }
  }
}
