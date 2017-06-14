package com.yixia.bigdata.etl

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

case class TdData (
     day:  Option[String],
     app:  Option[String],
     event_time: Option[String],
     idfa: Option[String],
     spread_name: Option[String],
     ip:   Option[String],
     device_type: Option[String],
     app_key:  Option[String],
     click_ip: Option[String],
     click_time: Option[String],
     adnet_name: Option[String],
     tdid: Option[String],
     ua:   Option[String]
   )

/**
  * Created by Administrator on 17/6/1.
  */
object TdChannelSpreadParser {

  def getFormatDate(timestamp: Option[String]) = {
    try{
      val sdf = new SimpleDateFormat("yyyyMMdd");
      val date = new Date(timestamp.get.toLong);
      Some(sdf.format(date));
    }catch{
      case e:Exception => println(s"Error with format date $timestamp")
        None
    }
  }

  def splitLogRaw(line: String) = {
    //try{
      val arr = line.split("&")
      val map = scala.collection.mutable.Map[String,String]()
      arr.map(kv => {
        val pairs = kv.split("=")
        if(pairs.length == 2 && !pairs(0).equalsIgnoreCase("UA")){
          //println(kv)
          //println(line)
          map.put(pairs(0).toLowerCase,pairs(1))
        }
      })
      val day:Option[String] = getFormatDate(map.get("eventtime"))
      //arr.map(println(_))
      TdData(day,
        map.get("app"),
        map.get("eventtime"),
        map.get("idfa"),
        map.get("spreadname"),
        map.get("ip"),
        map.get("devicetype"),
        map.get("appkey"),
        map.get("clickip"),
        map.get("clicktime"),
        map.get("adnetname"),
        map.get("tdid"),
        map.get("ua")
      )
    /*}catch{
      case e:Exception => println(s"Error record: $line")
        None
    }*/
  }


  def main(args: Array[String]) {
    /*val data="EVENTTIME=1495180697319&APPKEY=656DD93B4E3749A5B5A8907B80E0220A&IDFA=67855EAA-8DAC-42E7-B4BA-5B5A218881A1&app=yzb&IP=27.153.182.28&DEVICETYPE=iPhone7,1"
    val data1="CLICKIP=188.29.165.16&APPKEY=656DD93B4E3749A5B5A8907B80E0220A&ADNETNAME=爱普&app=yzb&SPREADNAME=爱普2&IDFA=30B75B56-B3D8-44C7-BB8B-09D45F21BA06&UA=Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1&EVENTTIME=1495128147390&CLICKTIME=1495124492000&IP=188.29.165.16&DEVICETYPE=iPhone9,4"
    splitLogRaw(data)
    println("---------")
    splitLogRaw(data1)*/

    val inputPath = "spark_1.6.3/src/main/resources/project/2017061110"
    val outputPath = "spark_1.6.3/src/main/resources/temporary"
    val part = 1
    val sc = new SparkContext("local[3]","td")

    /*val inputPath = args(0)
    val outputPath = args(1)
    val part = args(2)
    val sc = new SparkContext()*/

    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
    import sqlContext.implicits._

    val rdd = sc.textFile(inputPath: String).repartition(part)
    println(s"#### total ${rdd.count()}")

    //rdd.map(splitLogRaw(_)).toDF().write.parquet(outputPath: String)
    val df = rdd.map(splitLogRaw(_)).toDF()
    df.registerTempTable("td")
    sqlContext.sql("select * from td").show()
    //df.write.save(outputPath)

    sc.stop()
  }
}