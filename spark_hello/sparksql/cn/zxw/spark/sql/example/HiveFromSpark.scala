package cn.zxw.spark.sql.example

import com.google.common.io.{ByteStreams, Files}
import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object HiveFromSpark {
  case class Record(key: Int, value: String)

  val kv1Stream = HiveFromSpark.getClass.getResourceAsStream("kv1.txt")
  val kv1File = File.createTempFile("kv1", "txt")
  kv1File.deleteOnExit()
  ByteStreams.copy(kv1Stream, Files.newOutputStreamSupplier(kv1File))

  def main(args: Array[String]) {
    val sc = new SparkContext("local","HiveFromSpark")
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    
    println(kv1File.getAbsolutePath)
    hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    hiveContext.sql(s"LOAD DATA LOCAL INPATH '${kv1File.getAbsolutePath}' INTO TABLE src")

    println("Result of 'SELECT *': ")
    hiveContext.sql("SELECT * FROM src").collect().foreach(println)

    val count = hiveContext.sql("SELECT COUNT(*) FROM src").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    val rddFromSql = hiveContext.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    println("Result of RDD.map:")
    val rddAsStrings = rddFromSql.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }

    // You can also register RDDs as temporary tables within a HiveContext.
    val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
    rdd.toDF().registerTempTable("records")

    // Queries can then join RDD data with data stored in Hive.
    println("Result of SELECT *:")
    hiveContext.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").collect().foreach(println)

    sc.stop()
  }
}
