package cn.zxw.spark.streaming.api

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zhangxw
 */
object StatefulWordCount {
  //nc -lk 9999
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    ssc.checkpoint("checkpoint")
    
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
    
    val words = lines.flatMap { x => x.split("\\s") }
    val pairs = words.map { x => (x,1) }
    val wordCount = pairs.reduceByKey( _+_ )

    /**
      * 1.WordCount
      */
    wordCount.updateStateByKey[Int](updateFunc1).print()
    /**
      * 2.WordCount
      * Return a new "state" DStream where the state for each key is updated by applying
      * the given function on the previous state of the key and the new values of the key.
      * org.apache.spark.Partitioner is used to control the partitioning of each RDD.
      * @param updateFunc State update function. If `this` function returns None, then
      *                   corresponding state key-value pair will be eliminated.
      * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
      *                    DStream.
      * @param initialRDD initial state value of each key.
      * @tparam S State type
      */
    val initialRDD = ssc.sparkContext.parallelize(List(("hello",1)), numSlices=1)
    wordCount.updateStateByKey[Int](updateFunc2, new HashPartitioner(2),
                                    rememberPartitioner=true, initialRDD).print()
    
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * The update function will be called for each word,
    * with newValues having a sequence of 1’s (from the (word, 1) pairs)
    * and the runningCount having the previous count.
    * @return
    */
  def updateFunc1 = (newValues:Seq[Int], runningCount:Option[Int]) => {
    val currCount = newValues.foldLeft(0)(_+_)
    val statCount = runningCount.getOrElse(0)
    Some(currCount + statCount)
  }
  def updateFunc2 = (iterator:Iterator[(String,Seq[Int],Option[Int])]) => {
    iterator.map[(String,Int)](f => {
      val current = f._2.sum
      val previous = f._3.getOrElse(0)
      (f._1,current+previous)
    })
  }
}