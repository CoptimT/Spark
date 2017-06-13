package cn.zxw.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lx
 */
object ApiBasic {
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val conf = new SparkConf().setMaster(master).setAppName("ApiBasic")
    val sc = new SparkContext(conf)
    
    val tf1 = sc.textFile("/my/directory")
    val tf2 = sc.textFile("/my/directory/*.txt")
    val tf3 = sc.textFile("/my/directory/*.gz")
    
    val wtf = sc.wholeTextFiles("data.txt") //(filename, content) pairs
    

    
    val jobConf = new JobConf()
    val rdd1 = sc.hadoopRDD(jobConf, classOf[TextInputFormat], classOf[LongWritable],classOf[Text])
    val rdd2 = sc.hadoopFile("data.txt", classOf[TextInputFormat], classOf[LongWritable],classOf[Text])
    val config = new Configuration()
    val rdd3 = sc.newAPIHadoopRDD(config, classOf[NewTextInputFormat], classOf[LongWritable],classOf[Text])
    val rdd4 = sc.newAPIHadoopFile("data.txt", classOf[NewTextInputFormat], classOf[LongWritable],classOf[Text], config)
    
    rdd4.saveAsTextFile("")
    rdd4.saveAsObjectFile("")
    
    
  }
  //1. 文本文件(TextInputFormat)
  def textfile(): Unit ={
    sc.textFile(“file.txt”) //将本地文本文件加载成RDD
    sc.textFile(“directory/*.txt”) //将某类文本文件加载成RDD
    sc.textFile(“hdfs://nn:9000/path/file”) //hdfs文件或目录

    inputRdd = sc.textFile(“/data/input”)
    inputRdd = sc.textFile(“file:///data/input”)
    inputRdd = sc.textFile(“hdfs:///data/input”)
    inputRdd = sc.textFile(“hdfs://namenode:8020/data/input”)
  }

  //2. sequenceFile文件(SequenceFileInputFormat)
  def sequenceFile(): Unit ={
    val master = args(0)
    val conf = new SparkConf().setMaster(master).setAppName("ApiBasic")
    val sc = new SparkContext(conf)

    val sf1 = sc.sequenceFile[LongWritable,Text]("data.txt")
    val sf2 = sc.sequenceFile("data.txt",classOf[LongWritable],classOf[Text])
    sc.sequenceFile(“file.txt”) //将本地二进制文件加载成RDD
    sc.sequenceFile[String, Int] (“hdfs://nn:9000/path/file”)

    nums.saveAsSequenceFile(“hdfs://nn:8020/output”)
  }
  //3. 使用任意自定义的Hadoop InputFormat
  //3.1 官方InputFormat
  def hadoopInputFormat(): Unit ={
    //sc.hadoopFile(path, inputFmt, keyClass, valClass)
  }
  //3.2 自定义InputFormat
  def myInputFormat(): Unit ={
    // NLineInputFormat将文本文件中的N行作为一个input split，由一个Map Task处理
    // 创建一个JobConf，并设置输入目录及其他属性
    val jobConf = new JobConf(); FileInputFormat.setInputPaths(jobConf, inputPath)
    // 使用NLineInputFormat, 为了与hadoop 1.0 和2.0都兼容，我们同时设置了两个属性 jobConf.setInt("mapred.line.input.format.linespermap", 100) jobConf.setInt("mapreduce.input.lineinputformat.linespermap", 100)
    // key的类型是LongWritable, value类型是Text
    val inputFormatClass = classOf[NLineInputFormat]
    var hadoopRdd = sc.hadoopRDD(jobConf, inputFormatClass, classOf[LongWritable], classOf[Text])
  }

  //3.3 HBase
  def hbaseFile(): Unit ={
    //创建SparkContext
    val sparkConf = new SparkConf val sc = new SparkContext(conf )
    // 设置hbase configuration
    val hbaseConf = HBaseConfiguration.create() hbaseConf.addResource(new Path(“hbase-site.xml")) hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
      //创建hbase RDD
      val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
      //获取总行数
      val count = hBaseRDD.count()

      //写入HBase
      // 设置hbase configuration
      val hbaseConf = HBaseConfiguration.create() hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
      val rdd = sc.sequenceFile(“hdfs://nn:8020/input”).map(........) rdd.foreach(line => saveToHBase(line, conf, zkQuorum, hbaseTableName))
      def saveToHBase(rdd : RDD[((String, Long), Int)], conf: HbaseConfiguration, zkQuorum : String, tableName : String) = {
      val jobConfig = new JobConf(conf) jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName) jobConfig.setOutputFormat(classOf[TableOutputFormat])
      new PairRDDFunctions(rdd.map { case ((metricId, timestamp), value) =>
      createHBaseRow(metricId, timestamp, value) }).saveAsHadoopDataset(jobConfig) }
      def createHBaseRow(metricId : String, timestamp : Long, value : Int) = { val record = new Put(Bytes.toBytes(metricId + "~" + timestamp)) record.add(Bytes.toBytes("metric"), Bytes.toBytes("col"),
      Bytes.toBytes(value.toString))
      (new ImmutableBytesWritable, record)
      }
  }

}