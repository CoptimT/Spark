package cn.zxw.spark.sql.bases

import org.apache.spark.{SparkConf, SparkContext}

object DataFrameOperations {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext("local[2]","DataFrameOperations",conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    //import sqlContext.implicits._
    // Create the DataFrame
    val df = sqlContext.read.json("spark_1.6.3/src/main/resources/sql/people.json")

    // DataFrame Operations
    df.show()
    
    df.printSchema()
    
    df.select("name").show()
    
    df.select(df("name"),df("age") + 1).show()
    
    df.filter(df("age") > 30).show()
    
    df.groupBy("age").count().show()
    
    // Running SQL Queries Programmatically
    df.registerTempTable("people")
    val df1 = sqlContext.sql("SELECT * FROM people")
    df1.show()
    
    sc.stop()
  }
  
}