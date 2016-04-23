package cn.zxw.spark.core.hello

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  
  def main(args: Array[String]): Unit = {
    val file = args(0)
    var conf=new SparkConf().setAppName("Word Count")
    var sc=new SparkContext(conf)
    var lines=sc.textFile(file)
    var words=lines.flatMap(_.split("\\s+"))
    var count=words.countByValue()
    println(count)
  }
  
}