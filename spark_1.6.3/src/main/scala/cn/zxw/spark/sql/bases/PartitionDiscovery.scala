package cn.zxw.spark.sql.bases

import org.apache.spark.SparkContext

object PartitionDiscovery {
  def main(args: Array[String]): Unit = {
    val path = "spark_1.6.3/src/main/resources/sql/"
    val sc = new SparkContext("local","PartitionDiscovery")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    val df=sqlContext.read.parquet(path+"hive/users/")
    df.show()
    //| name|favorite_color|favorite_numbers|gender|country|
    sc.stop()
  }
}