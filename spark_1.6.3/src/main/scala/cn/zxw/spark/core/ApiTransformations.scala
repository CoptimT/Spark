package cn.zxw.spark.core

import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lx
 */
object ApiTransformations {
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val conf = new SparkConf().setMaster(master).setAppName("ApiBasic")
    val sc = new SparkContext(conf)
    
    
    
    
  }
}