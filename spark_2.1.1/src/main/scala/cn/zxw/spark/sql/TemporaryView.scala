package cn.zxw.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by zhangxw on 2017/8/16.
  */
object TemporaryView {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val path = "spark_2.1.1/src/main/resources/examples/"
    val people = spark.read.json(path+"people.json")
    // 1.TempView
    people.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    // 2.Register the DataFrame as a global temporary view
    people.createGlobalTempView("people1")

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people1").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people1").show()

    spark.stop()
  }
}
