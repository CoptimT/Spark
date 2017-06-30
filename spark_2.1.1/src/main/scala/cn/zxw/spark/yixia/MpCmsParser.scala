package cn.zxw.spark.yixia

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

case class MpCms (
     log_type:  Option[Long],
     server_ip: Option[String],
     client_ip: Option[String],
     timestamp: Option[Long],
     operator_id: Option[Long],
     operator_name:  Option[String],
     operator_object_id: Option[Long],
     operation_module:  Option[Long],
     operation_type: Option[Long],
     operator_before: Option[String],
     operator_after: Option[String]
   )

/**
  * Created by Administrator on 17/6/1.
  */
object MpCmsParser {

  def splitLogRaw(line: String) = {
    //2017-06-30T10:59:53.707615+08:00 iZ255igte3xZ nginx operator_id=544&operator_object_id=2017062809934580&operation_module=1&operation_type=107&operator_before=0&operator_after=1501430399&operator_name=李智梅&timestamp=1498791593707&log_type=1&server_ip=10.45.137.49&client_ip=111.202.106.229
    val arr = line.trim.split("&")
      val map = scala.collection.mutable.Map[String,String]()
      arr.map(kv => {
        val pairs = kv.split("=")
        if(pairs.length == 2){
          map.put(pairs(0).toLowerCase,pairs(1))
        }else{
          println(s"error $kv")
          println(line)
        }
      })
      MpCms(
        map.get("log_type").map(_.toLong),
        map.get("server_ip"),
        map.get("client_ip"),
        map.get("timestamp").map(_.toLong),
        map.get("operator_id").map(_.toLong),
        map.get("operator_name"),
        map.get("operator_object_id").map(_.toLong),
        map.get("operation_module").map(_.toLong),
        map.get("operation_type").map(_.toLong),
        map.get("operator_before"),
        map.get("operator_after")
      )
  }


  def main(args: Array[String]) {
    //test
    val inputPath = "spark_2.1.1/src/main/resources/project/mp_cms"
    val outputPath = "spark_2.1.1/src/main/resources/temporary"
    val part = 1
    val sc = new SparkContext("local[3]","mp_cms")

    /*val inputPath = args(0)
    val outputPath = args(1)
    val part = args(2)
    val sc = new SparkContext()*/

    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
    import sqlContext.implicits._

    val rdd = sc.textFile(inputPath: String).repartition(part)
    println(s"########## total ${rdd.count()} ##########")

    //test
    //rdd.take(100).foreach(println)
    //rdd.take(1).foreach(splitLogRaw)
    //rdd.filter(_.split("\\s").length == 4).map(_.split("\\s")(3)).map(splitLogRaw(_)).toDF().show()

    val df = rdd.filter(_.split("\\s").length == 4).map(_.split("\\s")(3)).map(splitLogRaw(_)).toDF()
    df.write.save(outputPath)

    sc.stop()
  }
}