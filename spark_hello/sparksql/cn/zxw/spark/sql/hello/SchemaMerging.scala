package cn.zxw.spark.sql.hello

import org.apache.spark.SparkContext

/**
 * error
 */
object SchemaMerging {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","SchemaMerging")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    // Create a simple DataFrame, stored into a partition directory
    val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.parquet("file/test_table/key=1")
    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.parquet("file/test_table/key=2")
    // Read the partitioned table
    val df3 = sqlContext.read.parquet("data/test_table")
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