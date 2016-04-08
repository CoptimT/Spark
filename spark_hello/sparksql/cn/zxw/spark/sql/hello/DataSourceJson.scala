package cn.zxw.spark.sql.hello

import org.apache.spark.SparkContext

object DataSourceJson {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","DataSourceJson")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files.
    val people = sqlContext.read.json("file/people.json") // File / Path / RDD[String]
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
    val df1 = sqlContext.sql("SELECT name,address.city FROM people")
    val df2 = sqlContext.sql("SELECT name,address.state FROM people")
    df.show()
    df1.show()
    df2.show()
  }
}