package cn.zxw.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lx
 */
object ApiBasic {
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val conf = new SparkConf().setMaster(master).setAppName("ApiBasic")
    val sc = new SparkContext(conf)
    
    val tf1 = sc.textFile("/my/directory")
    val tf2 = sc.textFile("/my/directory/*.txt")
    val tf3 = sc.textFile("/my/directory/*.gz")
    
    val wtf = sc.wholeTextFiles("data.txt") //(filename, content) pairs
    
    val sf1 = sc.sequenceFile[LongWritable,Text]("data.txt")
    val sf2 = sc.sequenceFile("data.txt",classOf[LongWritable],classOf[Text])
    
    val jobConf = new JobConf()
    val rdd1 = sc.hadoopRDD(jobConf, classOf[TextInputFormat], classOf[LongWritable],classOf[Text])
    val rdd2 = sc.hadoopFile("data.txt", classOf[TextInputFormat], classOf[LongWritable],classOf[Text])
    val config = new Configuration()
    val rdd3 = sc.newAPIHadoopRDD(config, classOf[NewTextInputFormat], classOf[LongWritable],classOf[Text])
    val rdd4 = sc.newAPIHadoopFile("data.txt", classOf[NewTextInputFormat], classOf[LongWritable],classOf[Text], config)
    
    rdd4.saveAsTextFile("")
    rdd4.saveAsObjectFile("")
    
    
  }

}