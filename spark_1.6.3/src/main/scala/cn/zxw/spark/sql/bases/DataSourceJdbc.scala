package cn.zxw.spark.sql.bases

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

object DataSourceJdbc {
  def main(args: Array[String]): Unit = {
    //spark-shell --jars /opt/spark-1.6.0/lib/mysql-connector-java-5.1.38.jar
    val sc = new SparkContext("local","DataSourceJdbc")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8"
    val username = "root"
    val password = "123456"
    val table = "tb_user"
    val map = Map(
      "driver" -> driver,
      "url" -> url,
      "dbtable" -> table,
      "user" -> username,
      "password" -> password)
    val properties = new Properties()
    properties.setProperty("driver", driver)
    properties.setProperty("user", username)
    properties.setProperty("password",password)

    //val jdbcDF = sqlContext.load("jdbc", map)
    //val jdbcDF = sqlContext.read.jdbc(url, table, properties)
    val jdbcDF = sqlContext.read.format("jdbc").options(map).load()
    jdbcDF.show()

    //Exception in thread "main" java.lang.RuntimeException: Table tb_user already exists.
    //jdbcDF.select(jdbcDF("username"),jdbcDF("password"),jdbcDF("age")).write.jdbc(url, table, properties)
    //success
    //jdbcDF.select(jdbcDF("username"),jdbcDF("password"),jdbcDF("age")).write.mode(SaveMode.Append).jdbc(url, table, properties)
    //Exception in thread "main" org.apache.spark.sql.AnalysisException: Table not found: tb_user;
    //jdbcDF.select(jdbcDF("username"),jdbcDF("password"),jdbcDF("age")).write.insertInto(table)

    sc.stop()
  }
}
/**
CREATE TABLE `tb_user` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `username` varchar(50) DEFAULT NULL,
  `password` varchar(20) DEFAULT NULL,
  `age` int(5) DEFAULT NULL,
  `status` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

mysql> insert into tb_user(username,password,age) values('Jane','123456',18);
mysql> insert into tb_user(username,password,age) values('Lily','qwertyuiop',28);
  */