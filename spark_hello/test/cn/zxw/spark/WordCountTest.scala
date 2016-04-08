package cn.zxw.spark

import org.apache.spark.SparkContext

/**
 * @author zhangxw
 */
object WordCountTest {
  
  def main(args: Array[String]): Unit = {
    test()
  }
  def test(){
    var sc = new SparkContext("local", "test")
    var rdd = sc.textFile("file\\my\\wordcount.txt", 1)
    var words = rdd.flatMap{line => line.split("\\s+")}.map(word => (word,1)).reduceByKey(_+_)
    println(words.toDebugString)
    println("-----------------")
    words.foreach(f => println(f._1+" = "+f._2))
    sc.stop()
    
  }
  
}