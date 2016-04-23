package cn.zxw.spark.core.hello

import org.apache.spark.SparkContext
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.examples.streaming.StreamingExamples

object TestApi {
  val path = "file/"
  val sc = new SparkContext("local","TestApi")
  def main(args: Array[String]): Unit = {
    //val logger = Logger.getLogger(TestApi.getClass)
    //logger.setLevel(Level.ERROR)
    StreamingExamples.setStreamingLogLevels()
    //testWC()
    //testAPI_1
    //testAPI_2
    //testAPI_3
    //testAPI_4
    //testAPI_5
    //leftOuterJoin
    //rightOuterJoin
    //fullOuterJoin
    //mapPartitions
    //mapPartitionsWithIndex
    //sample
    //intersection
    //distinct
    //aggregate
    //aggregateByKey
    //cogroup
    //cartesian
    //coalesce
    //repartition
    //repartitionAndSortWithinPartitions
    //takeSample
    //takeOrdered
    //zipWithIndex
    //zipWithUniqueId
    //zip
    //zipPartitions
    //subtract
    //randomSplit
    //glom
    //foreachPartition
    //sortBy
    //fold
    //mapValues
    //flatMapValues
    //combineByKey
    foldLeft
    
    sc.stop()
  }
  def foldLeft(){
    var seq = Seq[Int](1,2,3,4)
    //val res = seq.foldLeft[Int](1)((a,b) => a+b)
    //val res = seq.foldRight[Int](1)((a,b) => a+b)
    val res = seq.fold[Int](1)((a,b) => a+b)
    println(res)
  }
  def saveAsNewAPIHadoopFile(){
    var rdd = sc.makeRDD(Array(("A",1)))
    //val arr = rdd.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
    
    
  }
  def combineByKey(){
    var rdd1 = sc.makeRDD(Array(("A",1),("B",1),("C",1),("B",2),("A",2),("C",2),("B",3),("A",3),("C",3),("C",4)),2)
    rdd1.combineByKey(
             (v : Int) => v + "_",   
             (c : String, v : Int) => c + "@" + v,  
             (c1 : String, c2 : String) => c1 + "$" + c2
     ).foreach { println(_) }
  }
  def flatMapValues(){
    var rdd = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
    rdd.flatMapValues(f => f+"_").foreach { println(_) }
  }
  def mapValues(){
    var rdd = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
    rdd.mapValues(x => x + "_").foreach { print(_) }
  }
  def partitionBy(){
    var rdd = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
    var rdd2 = rdd.partitionBy(new org.apache.spark.HashPartitioner(3))
  }
  def fold(){
    var rdd1 = sc.makeRDD(1 to 4,2)
    val res = rdd1.fold(1)((x,y) => x+y)
    println(res)
  }
  def sortBy(){
    var rdd1 = sc.makeRDD(Seq(3,6,7,1,2,0),2)
    rdd1.sortBy(x => x).foreach { print(_) }
    println
    var rdd2 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))
    rdd2.sortBy(x => x._2,false).foreach { print(_) }
  }
  def foreachPartition(){
    var rdd1 = sc.makeRDD(1 to 10,2)
    rdd1.foreachPartition { itera => {
      for(i <- itera){
        print(i+" ")
      }
      println
    }}
  }
  def glom(){
    var rdd = sc.makeRDD(1 to 10,3)
    rdd.glom().foreach { arr => {
      for(i <- arr){
        print(i+" ")
      }
      println
    }}
  }
  def randomSplit(){
    var rdd = sc.makeRDD(1 to 10)
    val arr = rdd.randomSplit(Array(0.1,0.2,0.3,0.4),100)
    for(r <- arr){
      r.foreach {println(_)}
      println("-----------")
    }
  }
  def subtract(){
    var rdd1 = sc.makeRDD(Seq(1,2,2,3))
    var rdd2 = sc.makeRDD(3 to 4)
    rdd1.subtract(rdd2).foreach(println(_))
  }
  def zipPartitions(){
    var rdd1 = sc.makeRDD(1 to 4,2)
    var rdd2 = sc.makeRDD(Seq("X","A","B","C","D","E"),2)
    rdd1.zipPartitions(rdd2)((ite1,ite2) => {
      var list = List[String]()
      while(ite1.hasNext && ite2.hasNext){
        list::=(ite1.next()+"_"+ite2.next())
      }
      list.iterator
    }).foreach(println(_))
  }
  def zip(){
    var rdd1 = sc.makeRDD(1 to 5,2)
    var rdd2 = sc.makeRDD(Seq("A","B","C","D","E"),2)
    rdd1.zip(rdd2).foreach(f => println(f._1+" = "+f._2))
    rdd2.zip(rdd1).foreach(f => println(f._1+" = "+f._2))
    var rdd3 = sc.makeRDD(Seq("A","B","C","D","E"),3)
    rdd1.zip(rdd3).foreach(f => println(f._1+" = "+f._2))//如果两个RDD分区数不同，则抛出异常
  }
  def zipWithUniqueId(){
    var rdd1 = sc.makeRDD(Seq("A","B","C","D","E","F"),2)
    rdd1.zipWithUniqueId().foreach(f => println(f._1+" = "+f._2))
    //总分区数为2
    //第一个分区第一个元素ID为0，第二个分区第一个元素ID为1
    //第一个分区第二个元素ID为0+2=2，第一个分区第三个元素ID为2+2=4
    //第二个分区第二个元素ID为1+2=3，第二个分区第三个元素ID为3+2=5
  }
  def zipWithIndex(){
    var rdd = sc.makeRDD(Seq("A","B","R","D","F"),2)
    rdd.zipWithIndex().foreach(f => println(f._1+" = "+f._2))
  }
  def takeOrdered(){
    var rdd = sc.makeRDD(1 to 20)
    val arr = rdd.takeOrdered(10)
    for(i <- arr){
      print(i+" ")
    }
  }
  def takeSample(){
    var rdd = sc.makeRDD(1 to 20)
    val arr = rdd.takeSample(true, 10, 100)
    for(i <- arr){
      print(i+" ")
    }
  }
  def repartitionAndSortWithinPartitions(){
    var rdd = sc.parallelize(List(("A",1),("E",5),("B",2),("F",6),("C",3),("D",4)), 1)
    val partitioner = new HashPartitioner(2)
    rdd = rdd.repartitionAndSortWithinPartitions(partitioner)
    println(rdd.partitions.size)
    rdd.foreachPartition(ite => {
      println("----- start ----- ")
      ite.foreach(kv => println(kv._1+" = "+kv._2))
      println("-----  end  ----- ")
    })
  }
  def repartition(){
    var rdd = sc.makeRDD(1 to 10,2)
    println(rdd.partitions.size)
    rdd = rdd.repartition(1)
    println(rdd.partitions.size)
    rdd = rdd.repartition(3)
    println(rdd.partitions.size)
  }
  def coalesce(){
    var rdd = sc.makeRDD(1 to 10,2)
    println(rdd.partitions.size)
    rdd = rdd.coalesce(1)
    println(rdd.partitions.size)
    rdd = rdd.coalesce(3)
    println(rdd.partitions.size)
    rdd = rdd.coalesce(3,true)
    println(rdd.partitions.size)
  }
  /**
   * cartesian
   */
  def cartesian(){
    val rdd1 = sc.parallelize(List("A","B"),2)
    val rdd2 = sc.parallelize(List("C","D"),2)
    rdd1.cartesian(rdd2).collect().foreach(println(_))
    val rdd3 = sc.parallelize(List(("A",1),("B",2)),2)
    val rdd4 = sc.parallelize(List(("C",'c'),("D",'d')),2)
    rdd3.cartesian(rdd4).collect().foreach(println(_))
  }
  /**
   * cogroup
   */
  def cogroup(){
    val rdd1 = sc.parallelize(List(("A",1),("B",2),("C",3)),2)
    val rdd2 = sc.parallelize(List(("A",'a'),("C",'c'),("D",'d')),2)
    rdd1.cogroup(rdd2).collect().foreach(println(_))
  }
  /**
   * aggregateByKey
   */
  def aggregateByKey(){
    val rdd = sc.parallelize(List((1,2),(1,3),(1,4),(2,5)),2)
    def seqOp(a:Int,b:Int):Int={
      println("seqOp: "+a+"\t"+b)
      math.max(a, b)
    }
    def combOp(a:Int,b:Int):Int={
      println("combOp: "+a+"\t"+b)
      a+b
    }
    rdd.aggregateByKey(1)(seqOp, combOp).collect().foreach(f=>println(f))
  }
  /**
   * aggregate
   */
  def aggregate(){
    val rdd = sc.makeRDD(1 to 10,2)
    val res = rdd.aggregate(1)((x,y)=>x+y, (x,y)=>x+y)
    println(res)
  }
  /**
   * distinct
   */
  def distinct(){
    val rdd = sc.parallelize(List(1,2,3,2,3))
    rdd.foreach { println(_) }
    println("---------------")
    rdd.distinct().foreach { println(_) }
  }
  /**
   * intersection
   */
  def intersection(){
    val rdd1 = sc.makeRDD(1 to 3)
    val rdd2 = sc.makeRDD(2 to 4)
    rdd1.intersection(rdd2).foreach { println(_) }
  }
  /**
   * sample
   */
  def sample(){
    val rdd = sc.makeRDD(1 to 5, 2).sample(true, 0.5, 100)
    rdd.foreach { println(_)}
  }
  /**
   * mapPartitionsWithIndex
   */
  def mapPartitionsWithIndex(){
    val rdd = sc.makeRDD(1 to 5, 2)
    val rdd1 = rdd.mapPartitionsWithIndex((index,f) => {
      var count:Int = 0
      f.foreach { x => count+=x }
      List(index+"|"+count).iterator
      }, false)
    rdd1.foreach { println(_) }
    println(rdd1.partitions.size)
  }
  /**
   * mapPartitions
   */
  def mapPartitions(){
    //val rdd = sc.parallelize(List('a','a','a','b','b','c'), 1)
    //rdd.mapPartitions(f => f.map { x => (x,1) }, false).reduceByKey(_+_).sortByKey(false, 1).foreach(f => println(f._1+" = "+f._2))
    val rdd = sc.makeRDD(1 to 5, 2)
    val rdd1 = rdd.mapPartitions(f => {
      var count:Int = 0
      f.foreach { x => count+=x }
      List(count).iterator
      }, true)
    rdd1.foreach { println(_) }
    println(rdd1.partitions.size)
  }
  /**
   * word count
   */
  def testWC(){
    val rdd = sc.textFile(path+"my\\wordcount.txt", 1)
    val res = rdd.flatMap { line => line.split(" ") }
                 .map { word => (word,1) }
                 .reduceByKey(_+_)
                 .map(kv => (kv._2,kv._1))
                 .sortByKey(false,1)
                 .map(vk => (vk._2,vk._1))
                 .saveAsTextFile(path+"res\\")
  }
  /**
   * union
   */
  def testAPI_1(){
    val rdd1 = sc.parallelize(List(('a',1),('b',1)), 1)
    val rdd2 = sc.parallelize(List(('c',1),('d',1)), 1)
    val rdd = rdd1.union(rdd2)
    rdd.collect().foreach(f => println(f._1+" = "+f._2))
  }
  /**
   * groupByKey
   */
  def testAPI_2(){
    val rdd1 = sc.parallelize(List(('a',1),('b',2),('a',3),('b',4)), 1)
    rdd1.groupByKey().collect().foreach(f => println(f._1+" = "+f._2))
  }
  /**
   * leftOuterJoin
   */
  def leftOuterJoin(){
    val rdd1 = sc.parallelize(List(('a',1),('b',1),('a',2),('b',2),('c',11)), 1)
    val rdd2 = sc.parallelize(List(('a',3),('b',3),('a',4),('b',4),('d',12)), 1)
    val rdd = rdd1.leftOuterJoin(rdd2)
    rdd.collect().foreach(f => println(f._1+" = "+f._2))
  }
  /**
   * rightOuterJoin
   */
  def rightOuterJoin(){
    val rdd1 = sc.parallelize(List(('a',1),('b',1),('a',2),('b',2),('c',11)), 1)
    val rdd2 = sc.parallelize(List(('a',3),('b',3),('a',4),('b',4),('d',12)), 1)
    val rdd = rdd1.rightOuterJoin(rdd2)
    rdd.collect().foreach(f => println(f._1+" = "+f._2))
  }
  /**
   * join
   */
  def testAPI_3(){
    val rdd1 = sc.parallelize(List(('a',1),('b',1),('a',2),('b',2),('c',11)), 1)
    val rdd2 = sc.parallelize(List(('a',3),('b',3),('a',4),('b',4),('d',12)), 1)
    val rdd = rdd1.join(rdd2)
    rdd.collect().foreach(f => println(f._1+" = "+f._2))
  }
  /**
   * fullOuterJoin
   */
  def fullOuterJoin(){
    val rdd1 = sc.parallelize(List(('a',1),('b',1),('a',2),('b',2),('c',11)), 1)
    val rdd2 = sc.parallelize(List(('a',3),('b',3),('a',4),('b',4),('d',12)), 1)
    val rdd = rdd1.fullOuterJoin(rdd2)
    rdd.collect().foreach(f => println(f._1+" = "+f._2))
  }
  /**
   * reduce
   */
  def testAPI_4(){
    val rdd1 = sc.parallelize(List(1,2,3,4,5), 1)
    val rdd2 = sc.parallelize(List(("a",1),("b",2),("c",3),("d",4)), 1)
    val res1 = rdd1.reduce(_+_)
    println(res1)
    val res2 = rdd2.reduce((x,y) => (x._1+y._1,x._2+y._2))
    println(res2)
  }
  /**
   * lookup
   */
  def testAPI_5(){
    val rdd2 = sc.parallelize(List(("a",1),("b",2),("a",3),("b",4)), 1)
    val res2 = rdd2.lookup("a")
    res2.foreach { println(_) }
  }
}