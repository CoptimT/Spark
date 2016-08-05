package cn.zxw.spark.sql.hello

import org.apache.spark.SparkContext

object SchemaMerging {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","SchemaMerging")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    // Create a simple DataFrame, stored into a partition directory
    val df1 = sc.makeRDD(1 to 3).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.parquet("file/test_table/key=1")
    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val df2 = sc.makeRDD(3 to 5).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.parquet("file/test_table/key=2")
    // Read the partitioned table
    //val df3 = sqlContext.read.parquet("data/test_table") //need config mergeSchema
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("hdfs://10.10.25.14:8020/test/spark/tb_merge/")
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