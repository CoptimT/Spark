package cn.zxw.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by zhangxw on 2017/8/16.
  */
object BuildInDatasourceJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._





  }

}
