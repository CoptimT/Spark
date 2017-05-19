package cn.zxw.spark.sql.bases

import org.apache.spark.{SparkConf, SparkContext}

object SchemaMerging {
  def main(args: Array[String]): Unit = {
    val path = "spark_1.6.3/src/main/resources/sql/"
    val conf = new SparkConf().set("spark.sql.parquet.mergeSchema","true")
    val sc = new SparkContext("local","SchemaMerging",conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    // Create a simple DataFrame, stored into a partition directory
    val df1 = sc.makeRDD(1 to 3).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.parquet(path+"temp/test_table/key=1")
    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val df2 = sc.makeRDD(3 to 5).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.parquet(path+"temp/test_table/key=2")
    // Read the partitioned table,need config mergeSchema
    //val df3 = sqlContext.read.option("mergeSchema", "true").parquet(path+"temp/test_table")
    val df3 = sqlContext.read.parquet(path+"temp/test_table")

    df3.printSchema()
    // The final schema consists of all 3 columns in the Parquet files together
    // with the partitioning column appeared in the partition directory paths.
    // root
    // |-- single: int (nullable = true)
    // |-- double: int (nullable = true)
    // |-- triple: int (nullable = true)
    // |-- key : int (nullable = true)
  }
}