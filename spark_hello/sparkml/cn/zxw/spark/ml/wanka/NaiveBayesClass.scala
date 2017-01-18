package cn.zxw.spark.ml.wanka

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger,Level}
/**
/opt/spark-1.6.0/bin/spark-submit --total-executor-cores 4 --class cn.zxw.spark.ml.wanka.NaiveBayesClass mining-gender.jar
spark://10.10.25.13:7077 hdfs://U007:8020/test/mining/tags/tags.txt 
hdfs://U007:8020/test/mining/sample/ 2 hdfs://U007:8020/test/mining/model/
/opt/spark-1.6.0/bin/spark-submit --total-executor-cores 4 --class cn.zxw.spark.ml.wanka.NaiveBayesClass ming-gender.jar spark://10.10.25.13:7077 hdfs://U007:8020/test/mining/tags/tags.txt hdfs://U007:8020/test/mining/sample/ 2 hdfs://U007:8020/test/mining/model/
 */
object NaiveBayesClass {
  val SIGN = "\001"
  def main(args: Array[String]) : Unit = {
    //关闭干扰日志的输出
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    val master: String = args(0)
    //标签路径
    val tagFile: String = args(1)
    //样本数据源
    val sampleFile: String = args(2) // 0:imei,1:age,2:gender,3:tag,4:value
    //要分类项 
    val classType:Int = args(3).toInt //1:age 2:gender
    //模型保存地址
    val modelPath: String = args(4)
    
    val conf = new SparkConf().setAppName("NaiveBayesClass").setMaster(master)
    val sc = new SparkContext(conf)
    
    val tagArr = sc.textFile(tagFile).toArray()
    val length = tagArr.length
    
    val data = sc.textFile(sampleFile).map { line => (line.split(SIGN)(0),line) }.groupByKey()
    
    val parsedData = data.map { t =>
      val arr = new Array[String](length)
      for(i <- 0 to length-1){
        arr(i) = "0"
      }
      t._2.foreach { x => 
        val fields = x.split(SIGN)
        val index = tagArr.indexOf(fields(3))
        arr(index) = fields(4)
      }
      LabeledPoint(t._2.head.split(SIGN)(classType).toDouble, Vectors.dense(arr.map(_.toDouble)))
    }

    // Split data into training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    
    val model:NaiveBayesModel = NaiveBayes.train(parsedData, lambda = 1.0, modelType = "multinomial")
    val predictionAndLabel = parsedData.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / parsedData.count()
    //val model:NaiveBayesModel = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    
    //val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    //val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("### Prediction right rate is " + accuracy)
    
    // Save and load model
    //model.save(sc, modelPath)
    //val sameModel = NaiveBayesModel.load(sc, modelPath)
    sc.stop()
  }
}
