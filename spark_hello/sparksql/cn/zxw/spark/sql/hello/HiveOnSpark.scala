package cn.zxw.spark.sql.hello

import org.apache.spark.SparkContext

object HiveOnSpark {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","HiveOnSpark")
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sqlContext.sql("LOAD DATA LOCAL INPATH 'file/kv1.txt' INTO TABLE src")
    // Queries are expressed in HiveQL
    sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)
  }
}