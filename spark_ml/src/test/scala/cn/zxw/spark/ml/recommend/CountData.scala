package cn.zxw.spark.ml.recommend

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 2015/11/24.
  */
object CountData {
   case class Person(id:Int,cuid:String)
   case class User(id:Int,age:Int)
   def main(args : Array[String]): Unit ={
     //设置spark运行环境
     val conf = new SparkConf().setAppName("GameALS").setMaster("local[2]")
                                .set("spark.sql.inMemoryColumnarStorage.compressed","true")
                                .set("spark.sql.inMemoryColumnarStorage.batchSize","10000")
                                .set("spark.sql.parquet.compression.codec","snappy")
     val sc = new SparkContext(conf)
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     import sqlContext.implicits._

     val people = sc.textFile("E:\\wanka\\ml-100k\\user_id.data").map(_.split("\001")).map(p => Person(p(0).toInt,p(1))).toDF()
     people.registerTempTable("people")

     val user: DataFrame = sc.textFile("E:\\wanka\\ml-100k\\user.data").map(_.split("\t")).map(p => User(p(0).toInt,p(1).toInt)).toDF()
     user.registerTempTable("user")

     sqlContext.cacheTable("people")
     sqlContext.cacheTable("user")

     val sql: DataFrame = sqlContext.sql("select a.id,a.cuid,b.id,b.age from people a left join user b on a.id = b.id")
     sql.collect.foreach(println)
   }
 }
