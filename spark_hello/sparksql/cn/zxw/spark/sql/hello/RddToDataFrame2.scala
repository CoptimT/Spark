package cn.zxw.spark.sql.hello

import org.apache.spark.SparkContext

object RddToDataFrame2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","RddToDataFrame2")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val people = sc.textFile("file/people.txt")
    
    // The schema is encoded in a string
    val schemaString = "name age"
    
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.types.{StructType,StructField,StringType};
    
    // Generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    
    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    
    // Apply the schema to the RDD.
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people")
    
    val results = sqlContext.sql("SELECT name FROM people")
    // The columns of a row in the result can be accessed by field index or by field name.
    results.map(t => "Name: " + t(0)).collect().foreach(println)
    peopleDataFrame.printSchema()
    
    sc.stop()
  }
}