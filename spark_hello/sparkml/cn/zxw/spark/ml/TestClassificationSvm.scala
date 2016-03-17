package cn.zxw.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.SVMModel

object TestClassificationSvm {
  val path = "C:\\zhangxw\\workSpace\\Spark\\spark_hello\\file\\mllib\\"
  
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","svm")
    val data = sc.textFile(path+"sample_svm_data.txt")
    val input = data.map { 
      line => 
      val parts = line.split(" ")
      LabeledPoint(parts.head.toDouble,Vectors.dense(parts.tail.map { x => x.toDouble }))
    }
    
    val numIterations = 50
    val model:SVMModel = SVMWithSGD.train(input, numIterations)
    val predictRes = input.map { lp =>  
        val label = model.predict(lp.features)
        (label,lp.label)
    }
    
    val predictOK = predictRes.filter(tuple => tuple._1 == tuple._2).count()
    println(s"data size : ${input.count} , predict success : ${predictOK}")
    println("predict result : " + predictOK.toDouble/predictRes.count().toDouble)
    sc.stop()
  }
  
}