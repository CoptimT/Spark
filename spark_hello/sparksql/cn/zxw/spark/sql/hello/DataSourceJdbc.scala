package cn.zxw.spark.sql.hello

import org.apache.spark.SparkContext
/**
 * error
 */
object DataSourceJdbc {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","DataSourceJdbc")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val jdbcDF = sqlContext.load("jdbc", Map(
                "driver" -> "com.mysql.jdbc.Driver",
                "url" -> "jdbc:mysql://127.0.0.1:3306/test?username=root&password=zxw",
                "dbtable" -> "status_log",
                "username" -> "root",
                "password" -> "zxw"))
                
    //sqlContext.read.jd
    jdbcDF.show()
  }
}