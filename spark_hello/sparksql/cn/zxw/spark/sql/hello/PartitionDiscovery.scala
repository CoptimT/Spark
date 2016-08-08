package cn.zxw.spark.sql.hello

import org.apache.spark.SparkContext

object PartitionDiscovery {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","PartitionDiscovery")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val r1=sqlContext.read.parquet("hdfs://10.10.25.14:8020/test/spark/tb_bsdk/")
    // /test/spark/tb_bsdk/country=china/ide=eclipse
    r1.show()
    // |id|date|channel_id|dh_id|users|total|country|ide
    
  }
}