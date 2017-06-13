package cn.zxw.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  
  def main(args: Array[String]): Unit = {
    //testMaster(args)
    testLocal()
  }
  
  def testLocal(){
    val conf = new SparkConf().setMaster("local").setAppName("test")
    var sc = new SparkContext(conf)
    var rdd = sc.textFile("file\\my\\wordcount.txt", 1)
    var words = rdd.flatMap{line => line.split("\\s+")}.map(word => (word,1)).reduceByKey(_+_)
    println(words.toDebugString)
    words.foreach(f => println(f._1+" = "+f._2))
    sc.stop()
  }
  
  def testMaster(args: Array[String]): Unit = {
    val master = args(0)
    val file = args(1)
    val conf = new SparkConf().setMaster(master).setAppName("ApiExample")
    val sc = new SparkContext(conf)
    var lines = sc.textFile(file)
    var words = lines.flatMap(_.split("\\s+"))
    var count = words.countByValue()
    println(count)
    sc.stop()
  }
}