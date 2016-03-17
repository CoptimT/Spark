package cn.zxw.spark.core.example

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Usage: BroadcastTest [slices] [numElem] [broadcastAlgo] [blockSize]
 */
object BroadcastTest {
  def main(args: Array[String]) {

    val bcName = if (args.length > 2) args(2) else "Http"
    val blockSize = if (args.length > 3) args(3) else "4096"

    val sparkConf = new SparkConf()
        .set("spark.broadcast.factory", s"org.apache.spark.broadcast.${bcName}BroadcastFactory")
        .set("spark.broadcast.blockSize", blockSize)
    val sc = new SparkContext("local","Broadcast Test",sparkConf)
    
    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = (0 until num).toArray

    for (i <- 0 until 3) {
      println("Iteration " + i)
      println("===========")
      val startTime = System.nanoTime //System.nanoTime
      val barr1 = sc.broadcast(arr1)
      val observedSizes = sc.parallelize(1 to 10, slices).map(_ => barr1.value.size) //parallelize
      observedSizes.collect().foreach(i => println(i))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6)) //1E6=1*10^6=1000000
    }
    
    sc.stop()
  }
}
