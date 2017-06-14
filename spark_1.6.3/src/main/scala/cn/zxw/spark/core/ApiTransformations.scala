package cn.zxw.spark.core

import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lx
 */
object ApiTransformations {
  val conf = new SparkConf().setMaster("local[2]").setAppName("ApiTransformations")
  val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {

    
    
    
    sc.stop()
  }
  def testPipe(): Unit ={
    //rdd.pipe()
  }
}