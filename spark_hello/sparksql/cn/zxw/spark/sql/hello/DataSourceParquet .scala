package cn.zxw.spark.sql.hello

import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 * error
 * maybe incompatible with windows
 */
object DataSourceParquet  {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","DataSourceParquet")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    // Configuration
    sqlContext.setConf("spark.sql.sources.default", "parquet")
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "false")
    //sqlContext.sql("set spark.sql.parquet.binaryAsString=false")
    
    // Default source: spark.sql.sources.default
    //val df1 = sqlContext.read.parquet("file/users.parquet")
    //val df1 = sqlContext.read.load("file/users.parquet")
    val df1 = sqlContext.read.format("parquet").load("file/users.parquet")
    df1.show()
    df1.select("name", "favorite_color").write.save("file/temp/namesAndFavColors.parquet")
    df1.select("name", "favorite_color").write.mode(SaveMode.Overwrite).parquet("file/temp/namesAndFavColors.parquet")
    
    // Manually Specifying Options
    val df2 = sqlContext.read.format("json").load("file/people.json")
    df2.select("name", "age").write.format("parquet").save("file/temp/namesAndAges.parquet")

    sc.stop()
  }
}