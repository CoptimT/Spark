package cn.zxw.spark.sql.hello

import org.apache.spark.SparkContext

/**
 * error
 */
object RddToDataFrame1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","RddToDataFrame1")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    // Define the schema using a case class.
    // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
    // you can use custom classes that implement the Product interface.
    case class Person(name: String, age: Int) extends Product
    
    // Create an RDD of Person objects and register it as a table.
    val rdd = sc.textFile("file/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    //val people = rdd.toDF() //error
    //people.registerTempTable("people")
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
    
    // The columns of a row in the result can be accessed by field index:
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
    // or by field name:
    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)
    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
    // Map("name" -> "Justin", "age" -> 19)
  }
}