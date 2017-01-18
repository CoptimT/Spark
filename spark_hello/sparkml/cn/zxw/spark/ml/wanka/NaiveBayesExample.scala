package cn.zxw.spark.ml.wanka

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.{SparkConf, SparkContext}

object NaiveBayesExample {

  def main(args: Array[String]) : Unit = {
    //val conf = new SparkConf().setAppName("NaiveBayesExample")
    //val sc = new SparkContext(conf)
    val sc = new SparkContext("local","NaiveBayesExample")
    val pkg = sc.textFile("file/temp/pkg.txt").toArray()
    val length = pkg.length
    
    val ex1 = sc.textFile("file/temp/ex1.txt")
    val data = ex1.map { line => (line.split("\t")(0),line) }.groupByKey()
    
    val parsedData = data.map { t =>
      val arr = new Array[String](length)
      for(i <- 0 to length-1){
        arr(i) = "0"
      }
      t._2.foreach { x => 
        val fields = x.split("\t")
        val index = pkg.indexOf(fields(3))
        arr(index) = fields(2)
      }
      LabeledPoint(t._2.head.split("\t")(1).toDouble, Vectors.dense(arr.map(_.toDouble)))
    }

    // Split data into training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println(accuracy)
    // Save and load model
    model.save(sc, "file/temp/myNaiveBayesModel")
    //val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
    sc.stop()
    // $example off$
  }
}
