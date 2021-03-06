package cn.zxw.spark.sql.bases

import org.apache.spark.SparkContext

object DataSourceJson {
  def main(args: Array[String]): Unit = {
    val path = "spark_1.6.3/src/main/resources/sql/"

    val sc = new SparkContext("local","DataSourceJson")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files.
    val people = sqlContext.read.json(path+"people.json") // File / Path / RDD[String]
    people.printSchema()
    people.show()
    // root
    //  |-- age: integer (nullable = true)
    //  |-- name: string (nullable = true)
    
    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // an RDD[String] storing one JSON object per string.
    val anotherPeopleRDD = sc.parallelize("""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sqlContext.read.json(anotherPeopleRDD)
    anotherPeople.printSchema()
    anotherPeople.registerTempTable("people")

    val df = sqlContext.sql("SELECT name,address FROM people")
    df.show()
    val df1 = sqlContext.sql("SELECT name,address.city FROM people")
    df1.show()
    val df2 = sqlContext.sql("SELECT name,address.state FROM people")
    df2.show()
    
    sc.stop()
  }
}