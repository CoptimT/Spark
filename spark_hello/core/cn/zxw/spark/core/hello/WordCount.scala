package cn.zxw.spark.core.hello

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author zhangxw
 */
object WordCount {
  
  def main(args: Array[String]): Unit = {
    var conf=new SparkConf().setAppName("Word Count")
    var sc=new SparkContext(conf)
    var lines=sc.textFile("/user/root/README.md")
    var words=lines.flatMap(_.split("\\s+"))
    var count=words.countByValue()
    println(count)
    
  }
  
}