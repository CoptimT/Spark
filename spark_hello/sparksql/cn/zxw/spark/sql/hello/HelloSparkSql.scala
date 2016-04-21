package cn.zxw.spark.sql.hello

import org.apache.spark.SparkContext

object HelloSparkSql {
  
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","HelloSparkSql")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    
    // Create the DataFrame
    val df = sqlContext.read.json("file/people.json")
    
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