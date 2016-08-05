package cn.zxw.spark.sql.hello

import org.apache.spark.SparkContext

object DataSourceJdbc {
  def main(args: Array[String]): Unit = {
    //spark-shell --jars /opt/spark-1.6.0/lib/mysql-connector-java-5.1.38.jar
    val sc = new SparkContext("local","DataSourceJdbc")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val jdbcDF = sqlContext.load("jdbc", Map(
                "driver" -> "com.mysql.jdbc.Driver",
                "url" -> "jdbc:mysql://10.10.25.13:3307/test?useUnicode=true&characterEncoding=utf-8",
                "dbtable" -> "tb_bsdk_count_redirect",
                "user" -> "root",
                "password" -> ""))
                
    //sqlContext.read.jdbc(url, table, properties)
    jdbcDF.show()
    sc.stop()
  }
}