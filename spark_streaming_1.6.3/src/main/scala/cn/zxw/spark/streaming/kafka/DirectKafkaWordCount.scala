package cn.zxw.spark.streaming.kafka

import java.net.InetAddress

import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper._

import scala.reflect.ClassTag
import scala.util.control.NonFatal

object DirectKafkaWordCount extends Logging{
  /**
    * 启动Job的时侯设定如下参数
    * topic = args(0)          设置kafka的请求主题
    * brokers = args(1)        设置kafka的broker列表，多个以逗号分隔"192.168.37.3:9092"
    * groupId = args(2)        设置kafka的消费者组id
    * interval = args(3).toInt 设置spreaming的处理间隔，以秒为单位
    * zkQuorum = args(4)       设置zk地址
    * partitions = args(5)     设置kafka中topic的分区数
    */
  def main(args: Array[String]) {
    val args = Array("test","10.30.2.8:9092","test","5","10.30.2.8:2181","1")
    if (args.length < 6) {
      System.err.println("Params not enough!!!" + args)
      System.exit(1)
    }
    val Array(topic, brokers, group, interval, zkQuorum, partitions) = args

    val sparkConf = new SparkConf().setAppName("DirectKafkaStream").setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf, Seconds(interval.toInt))
    
    // Create direct kafka stream with brokers and topics
    val topicsSet = Set(topic)
    val kafkaParams = Map[String, String](
      "group.id" -> group,
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )

    val fromOffsets = getFromOffsets(zkQuorum,topic,group,partitions.toInt)

    /*createDirectStream[K, V, KD <:kafka.serializer.Decoder[K], VD <:kafka.serializer.Decoder[V], R](
     ssc: StreamingContext,
     kafkaParams: Map[String, String],
     fromOffsets: Map[TopicAndPartition, Long],
     messageHandler: MessageAndMetadata[K, V] => R): InputDStream[R]*/

    /*val dstream: DStream[(String, Array[Byte])] = fromOffsets.map { fromOffset =>
      val messageHandler: MessageAndMetadata[String, Array[Byte]] => (String, Array[Byte]) = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.key, mmd.message)
      KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, Tuple2[String, Array[Byte]]](ssc, kafkaParams, fromOffset, messageHandler)
    }.getOrElse{
      KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
    }*/
    val dstream: DStream[(String, String)] = fromOffsets.map { fromOffset =>
      println(fromOffset)
      val messageHandler: MessageAndMetadata[String, String] => (String, String) = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, Tuple2[String, String]](ssc, kafkaParams, fromOffset, messageHandler)
    }.getOrElse{
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    }

    //val dstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    dstream.foreachRDDWithOffsets(zkQuorum,group,topic){ rdd =>
      val wc = rdd.map(_._2).flatMap(_.split("\\s")).map(x => (x, 1L)).reduceByKey(_ + _)
      wc.collect().foreach(t => println(t._1 + ": " + t._2))
    }
    
    ssc.start()
    ssc.awaitTermination()
  }

  //=================================================================================================================
  //以下offsets存入zk代码根据样例改写:http://geeks.aretotally.in/spark-streaming-kafka-direct-api-store-offsets-in-zk/
  //=================================================================================================================
  def getFromOffsets(zkQuorum: String, topic: String, group: String, partitions: Int): Option[Map[TopicAndPartition, Long]] = {
    val zkClient: ZooKeeper = new ZooKeeper(zkQuorum, 3000, new Watcher {
      override def process(event: WatchedEvent): Unit = {}
    })
    val list = (0 until partitions).toList.flatMap { i =>
      val nodePath = s"/consumers/$group/offsets/$topic/$i"
      try {
        zkClient.exists(nodePath,false) match {
          case stat:Stat =>
            val maybeOffset = Option(new String(zkClient.getData(nodePath,false,null)))
            logInfo(s"Kafka Direct Stream - From Offset - ZK Node ($nodePath) - Value: $maybeOffset")
            maybeOffset.map { offset =>
              TopicAndPartition(topic, i) -> offset.toLong
            }
          case _ =>
            logInfo(s"Kafka Direct Stream - From Offset - ZK Node ($nodePath) does NOT exist")
            None
        }
      } catch {
        case NonFatal(error) =>
          logError(s"Kafka Direct Stream - From Offset - ZK Node ($nodePath) - Error: $error")
          None
      }
    }
    zkClient.close()

    (list.size == partitions) match {
      case true =>
        list.foreach {
          case (t, offset) =>
            logInfo(s"From Offset - Topic: ${t.topic}, Partition: ${t.partition}, Offset: ${offset}")
        }
        Some(list.toMap)
      case false =>
        logInfo(s"Current ZK offsets (${list.size} doesn't match partition size, so gonna rely on Spark checkpoint to reuse Kafka offsets")
        None
    }
  }

  implicit class PimpDStream[T: ClassTag](dstream: DStream[T]) extends Serializable {

    def foreachRDDWithOffsets(zkQuorum: String, group: String, topic: String)(f: RDD[T] => Unit): Unit = {
      dstream.foreachRDD { rdd =>
        // Connect to ZK
        implicit val zk: ZooKeeper = new ZooKeeper(zkQuorum, 3000, new Watcher {
          override def process(event: WatchedEvent): Unit = {}
        })
        try {
          // Try to store offsets in ZK
          try {
            rdd.zkOffsets(group)
          } catch {
            case NonFatal(error) =>
              logError(s"DStream Foreach RDD with Offset - Store Offsets - Error: $error")
          }

          f(rdd) // Do the work
        } catch {
          case NonFatal(error) =>
            logError(s"DStream Foreach RDD with Offset - Error: $error")
            throw error
        } finally {
          // Always cleanup ZK connections
          zk.close()
        }
      }
    }
  }

  //问题：zk不能创建一次创建多级目录
  implicit class PimpRDD[T: ClassTag](rdd: RDD[T]) {

    def zkOffsets(group: String)(implicit zk: ZooKeeper): Unit = {
      rdd match {
        case offsetRdd: HasOffsetRanges =>
          val offsets = offsetRdd.offsetRanges
          offsets.foreach { o =>
            // Consumer Offset
            locally {
              val nodePath = s"/consumers/$group/offsets/${o.topic}/${o.partition}"

              logInfo(s"Kafka Direct Stream - Offset Range - Topic: ${o.topic}, Partition: ${o.partition}, From Offset: ${o.fromOffset}, To Offset: ${o.untilOffset}, ZK Node: $nodePath")

              zk.exists(nodePath,false) match {
                case stat:Stat =>
                  logInfo(s"Kafka Direct Stream - Offset - ZK Node ($nodePath) exists, setting value: ${o.untilOffset}")
                  /**
                    * Set the data for the node of the given path if such a node exists and the
                    * given version matches the version of the node (if the given version is
                    * -1, it matches any node's versions). Return the stat of the node.
                    */
                  zk.setData(nodePath, o.untilOffset.toString.getBytes,-1)

                case _ =>
                  logInfo(s"Kafka Direct Stream - Offset - ZK Node ($nodePath) does NOT exist -- setting value: ${o.untilOffset}")
                  zk.create(nodePath, o.untilOffset.toString.getBytes,  ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
              }
            }

            val hostname = InetAddress.getLocalHost().getHostName()
            val ownerId = s"${group}-${hostname}-${o.partition}"
            val now = org.joda.time.DateTime.now.getMillis

            // Consumer Ids
            locally {
              val nodePath = s"/consumers/$group/ids/${ownerId}"
              val value = s"""{"version":1,"subscription":{"${o.topic}":${o.partition},"pattern":"white_list","timestamp":"$now"}}"""

              zk.exists(nodePath,false) match {
                case stat:Stat =>
                  logInfo(s"Kafka Direct Stream - Id - ZK Node ($nodePath) exists, setting value: ${value}")
                  zk.setData(nodePath, value.getBytes, -1)

                case _ =>
                  logInfo(s"Kafka Direct Stream - Id - ZK Node ($nodePath) does NOT exist -- setting value: ${value}")
                  zk.create(nodePath, value.getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
              }
            }

            // Consumer Owners
            locally {
              val nodePath = s"/consumers/$group/owners/${o.topic}/${o.partition}"
              val value = ownerId

              zk.exists(nodePath,false) match {
                case stat:Stat =>
                  logInfo(s"Kafka Direct Stream - Owner - ZK Node ($nodePath) exists, setting value: ${value}")
                  zk.setData(nodePath, value.getBytes, -1)

                case _ =>
                  logInfo(s"Kafka Direct Stream - Owner - ZK Node ($nodePath) does NOT exist -- setting value: ${value}")
                  zk.create(nodePath, value.getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
              }
            }
          }
        case _ =>
          logError(s"DStream - ZK Offsets - Cannot store on ZK since RDD is not of type of HasOffsetRanges: $rdd")
      }
    }
  }

  //=================================================================================================================
  //以下offsets存入zookeeper样例摘录自:http://geeks.aretotally.in/spark-streaming-kafka-direct-api-store-offsets-in-zk/
  //=================================================================================================================
  /*
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
        case NonFatal(error) =>
          logger error s"Kafka Direct Stream - From Offset - ZK Node ($nodePath) - Error: $error"
          None
      }
    }

    (list.size == partitions) match {
      case true =>
        list.foreach {
          case (t, offset) =>
            logger info s"From Offset - Topic: ${t.topic}, Partition: ${t.partition}, Offset: ${offset}"
        }
        Some(list.toMap)
      case false =>
        logger info s"Current ZK offsets (${list.size} doesn't match partition size, so gonna rely on Spark checkpoint to reuse Kafka offsets"
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
  */

}
