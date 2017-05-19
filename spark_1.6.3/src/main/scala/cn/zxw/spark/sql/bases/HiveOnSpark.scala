package cn.zxw.spark.sql.bases

import org.apache.spark.SparkContext

object HiveOnSpark {
  def main(args: Array[String]): Unit = {
    val path = "spark_1.6.3/src/main/resources/sql/"
    val sc = new SparkContext("local","HiveOnSpark")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // Queries are expressed in HiveQL
    hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    hiveContext.sql("LOAD DATA LOCAL INPATH '"+path+"kv1.txt' INTO TABLE src")
    hiveContext.sql("FROM src SELECT key, value").show()
    
    val tableName = "test"
    val df = hiveContext.table(tableName)
    df.saveAsTable(tableName)
    
    sc.stop()
  }
}