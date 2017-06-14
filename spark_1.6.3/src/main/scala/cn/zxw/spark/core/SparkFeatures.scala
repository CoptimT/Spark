package cn.zxw.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhangxw on 2017/6/13.
  */
object SparkFeatures {
  val conf = new SparkConf().setMaster("local[2]").setAppName("ApiTransformations")
  val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
    accumulator()
    broadcast()
    cache()

    sc.stop()
  }
  // Accumulator(累加器，计数器)
  def accumulator(): Unit ={
    import SparkContext._
    /*val total_counter = sc.accumulator(0L, "total_counter")
    val counter0 = sc.accumulator(0L, "counter0")
    val counter1 = sc.accumulator(0L, "counter1")
    sc.parallelize(1 to 100, 2).map[Int]{ i =>
      total_counter += 1
      val x = math.random * 2 - 1
      val y = math.random * 2 – 1
      if ((x*x + y * y) < 1) {
          counter1 += 1
      } else {
          counter0 += 1
      }
      if ((x*x + y*y) < 1) 1 else 0
    }.reduce(_ + _)*/
  }

  //  广播变量
  def broadcast(): Unit ={
    val data = Set(1, 2, 4, 6) // 大小为128MB
    val bdata = sc.broadcast(data)
    val rdd = sc.parallelize(1 to 1000000, 100)
    val observedSizes= rdd.map(_ => bdata.value.size )
  }

  //cache
  def cache(): Unit ={
    val data = sc.textFile("hdfs://nn:8020/input")
    data.cache()
    data.filter(_.startsWith("error")).count
    data.filter(_.endsWith("hadoop")).count
    data.filter(_.endsWith("hbase")).count
  }


}
