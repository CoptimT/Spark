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
object NaiveBayesClass1 {
  val SIGN = "\001"
  val malePath: String = "hdfs://U007:8020/test/mining/male/male.txt"
  val femalePath: String = "hdfs://U007:8020/test/mining/female/female.txt"
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
    
    val maleData = sc.textFile(malePath).map { line => (line.split(SIGN)(0),line) }.groupByKey()
    val maleLP = maleData.map { t =>
      val arr = new Array[Double](length)
      for(i <- 0 to length-1){
        arr(i) = 2
      }
      t._2.foreach { x => 
        val fields = x.split(SIGN)
        val index = tagArr.indexOf(fields(3))
        arr(index) = fields(4).toDouble
      }
      LabeledPoint(1, Vectors.dense(arr))
    }

    val femaleData = sc.textFile(femalePath).map { line => (line.split(SIGN)(0),line) }.groupByKey()
    val femaleLP = femaleData.map { t =>
      val arr = new Array[Double](length)
      for(i <- 0 to length-1){
        arr(i) = 2
      }
      t._2.foreach { x => 
        val fields = x.split(SIGN)
        val index = tagArr.indexOf(fields(3))
        arr(index) = fields(4).toDouble
      }
      LabeledPoint(0, Vectors.dense(arr))
    }
    
    // Split data into training (60%) and test (40%).
    val splitsMale = maleLP.randomSplit(Array(0.7, 0.3), seed = 18L)
    val trainingMale = splitsMale(0)
    val testMale = splitsMale(1)
    
    val splitsFemale = femaleLP.randomSplit(Array(0.7, 0.3), seed = 15L)
    val trainingFemale = splitsFemale(0)
    val testFemale = splitsFemale(1)
    
    val training = trainingFemale.union(trainingMale)
    val test = testFemale.union(testMale)
    println("### training data view " + training.take(1)(0).features.toArray.mkString(","))
    println("### test data view " + test.take(1)(0).features.toArray.mkString(","))
    println("### maleLP " + maleLP.count())
    println("### trainingMale " + trainingMale.count())
    println("### testMale " + testMale.count())
    println("### femaleLP " + femaleLP.count())
    println("### trainingFemale " + trainingFemale.count())
    println("### testFemale " + testFemale.count())
    println("### training " + training.count())
    println("### test " + test.count())
    
    val model:NaiveBayesModel = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    
    val predictionAndLabel = test.map(p => (p.label,model.predict(p.features)))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("### predictionAndLabel data :")
    //predictionAndLabel.take(30).foreach(t => println(t._1 + " , " + t._2))
    predictionAndLabel.groupByKey().foreach( t =>
      println(t._1 + ": " + t._2.count(p => true) + " , " + t._2.filter { x => x == t._1 }.count(p => true))
    )
    println("### predictionAndLabel " + predictionAndLabel.count())
    println("### Prediction right rate is " + accuracy)
    
    // Save and load model
    //model.save(sc, modelPath)
    //val sameModel = NaiveBayesModel.load(sc, modelPath)
    sc.stop()
  }
}
