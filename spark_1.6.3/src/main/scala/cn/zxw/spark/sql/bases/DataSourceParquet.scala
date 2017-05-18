package cn.zxw.spark.sql.bases

import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

object DataSourceParquet  {
  def main(args: Array[String]): Unit = {
    val path = "spark_1.6.3/src/main/resources/sql/"

    val sc = new SparkContext("local","DataSourceParquet")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.implicits._

    // Configuration
    //sqlContext.setConf("spark.sql.sources.default", "parquet")
    //sqlContext.setConf("spark.sql.parquet.binaryAsString", "false")
    //sqlContext.sql("set spark.sql.parquet.binaryAsString=false")
    
    // Default source: spark.sql.sources.default
    //val df1 = sqlContext.read.parquet(path+"users.parquet")
    //val df1 = sqlContext.read.load(path+"users.parquet")
    val df1 = sqlContext.read.format("parquet").load(path+"users.parquet")
    df1.show()
    df1.select("name", "favorite_color").write.save(path+"temp/namesAndFavColors.parquet")
    df1.select("name", "favorite_color").write.mode(SaveMode.Overwrite).parquet(path+"temp/namesAndFavColors.parquet")
    
    // Manually Specifying Options
    val df2 = sqlContext.read.format("json").load(path+"people.json")
    df2.select("name", "age").write.format("parquet").save(path+"temp/namesAndAges.parquet")

    sc.stop()
  }
}