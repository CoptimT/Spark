package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

object ExceptionHandlingTest {
  def main(args: Array[String]) {
    val sc = new SparkContext("local","ExceptionHandlingTest")
    
    sc.parallelize(0 until 30).foreach { i =>
      if (math.random > 0.75) {
        println(i)
        throw new Exception("Testing exception handling")
      }else{
        println(i)
      }
    }
    
    sc.stop()
  }
}
