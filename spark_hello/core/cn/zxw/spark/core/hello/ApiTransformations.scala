package cn.zxw.spark.core.hello

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat=>NewTextInputFormat}

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