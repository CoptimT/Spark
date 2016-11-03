package cn.zxw.spark.streaming.hello

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.HashPartitioner

/**
 * @author zhangxw
 */
object StatefulWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)//Level.WARN
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    ssc.checkpoint("D:\\GitHub\\Spark\\spark_hello\\file\\checkpoint\\")
    
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY_SER_2)
    
    val words = lines.flatMap { x => x.split("\\s") }
    val pairs = words.map { x => (x,1) }
    val wordCount = pairs.reduceByKey( _+_ )
    
    //1
    wordCount.updateStateByKey[Int](updateFunc1).print()
    //2
    val initialRDD = ssc.sparkContext.parallelize(List(("hello",1)), numSlices=1)
    wordCount.updateStateByKey[Int](updateFunc2, new HashPartitioner(2), 
        rememberPartitioner=true, initialRDD).print()
    
    ssc.start()
    ssc.awaitTermination()
  }
  
  def updateFunc1 = (values:Seq[Int], state:Option[Int]) => {
    val currCount = values.foldLeft(0)(_+_)
    val statCount = state.getOrElse(0)
    Some(currCount + statCount)
  }
  def updateFunc2 = (iterator:Iterator[(String,Seq[Int],Option[Int])]) => {
    iterator.map[(String,Int)](f=>{
      val current = f._2.sum
      val previous = f._3.getOrElse(0)
      (f._1,current+previous)
    })
  }
  
}