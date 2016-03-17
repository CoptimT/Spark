package cn.zxw.spark.ml

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.random.RandomRDDs

object TestBasicCount extends App{
  //summary
  //relation
  //sample
  //run
  
  
  /**
   * 3.1 summary
   */
  def summary(){
    val sc = new SparkContext("local", "summary")
    val v0: Vector = Vectors.dense(Array(0.0, 0.0, 0.0))
    val v1: Vector = Vectors.dense(Array(1.0, 2.0, 3.0))
    val v2: Vector = Vectors.dense(Array(1.0, 22.0, 33.0))
    val v3: Vector = Vectors.dense(Array(2.0, 200.0, 300.0))
    val arr = Array(v0,v1,v2,v3)
    val rdd: RDD[Vector] = sc.parallelize(arr)
    val mss:MultivariateStatisticalSummary = Statistics.colStats(rdd)
    println(mss.count)      //总数
    println(mss.max)
    println(mss.min)
    println(mss.mean)       //平均数
    println(mss.variance)   //方差
    println(mss.numNonzeros)//非零个数
  }
  
  
  /**
   * 3.2 relation
   */
  def relation(){
    val sc = new SparkContext("local", "relation")
    val arr1 = Array(1.0, 2.0, 3.0)
    val arr2 = Array(1.1, 2.2, 3.3)
    val rdd1 = sc.parallelize(arr1)
    val rdd2 = sc.parallelize(arr2)
    val relation1 = Statistics.corr(rdd1, rdd2, "pearson")
    println(relation1)
    val v1: Vector = Vectors.dense(Array(1.0, 2.0, 3.0))
    val v2: Vector = Vectors.dense(Array(11.0, 22.0, 33.0))
    val rdd3 = sc.parallelize(Array(v1, v2))
    val relation2 = Statistics.corr(rdd3, "pearson")
    println(relation2)
    val relation3 = Statistics.corr(rdd3, "spearman")
    println("---------------------------------------")
    println(relation3)
  }
  
  
  /**
   * 3.3 sample
   */
  def sample(){
    val sc = new SparkContext("local", "sample")
    val arr1 = Array((1,1.0),(1,2.0),(1,3.0),(1,10.0),(1,20.0),(1,30.0),(2,3.0),(2,33.0),(2,333.0),(2,3333.0),(2,33333.0))
    val rdd1 = sc.parallelize(arr1)
    val res1 = rdd1.sampleByKey(withReplacement=false, fractions=Map(1 -> 0.5,2 -> 0.6), seed=1000)
    val res2 = rdd1.sampleByKeyExact(withReplacement=false, fractions=Map(1 -> 0.5,2 -> 0.6))
    res1.collect().foreach(println(_))
    println("---------------------------------------")
    res2.collect().foreach(println(_))
    
  }
  
  
  /**
   * 3.3 example sample
   */
  def run() {
    val sc = new SparkContext("local", "sample")
    val fraction = 0.1 // fraction of data to sample

    val examples = MLUtils.loadLibSVMFile(sc,"C:\\zhangxw\\workSpace\\Spark\\spark_hello\\file\\mllib\\sample_libsvm_data.txt")
    val numExamples = examples.count()
    if (numExamples == 0) {
      throw new RuntimeException("Error: Data file had no samples to load.")
    }
    println(s"Loaded data with $numExamples examples from file: /sample_libsvm_data.txt")
    
    // Example: RDD.sample() and RDD.takeSample()
    val expectedSampleSize = (numExamples * fraction).toInt
    println(s"Sampling RDD using fraction $fraction.  Expected sample size = $expectedSampleSize.")
    val sampledRDD = examples.sample(withReplacement = true, fraction = fraction)
    println(s"RDD.sample(): sample has ${sampledRDD.count()} examples")
    val sampledArray = examples.takeSample(withReplacement = true, num = expectedSampleSize)
    println(s"RDD.takeSample(): sample has ${sampledArray.size} examples")
    println()

    // Example: RDD.sampleByKey() and RDD.sampleByKeyExact()
    val keyedRDD = examples.map { lp => (lp.label.toInt, lp.features) }
    println(s"Keyed data using label (Int) as key ==> Orig")
    //  Count examples per label in original data.
    val keyCounts = keyedRDD.countByKey()
    
    //  Subsample, and count examples per label in sampled data. (approximate)
    val fractions = keyCounts.keys.map((_, fraction)).toMap
    val sampledByKeyRDD = keyedRDD.sampleByKey(withReplacement = true, fractions = fractions)
    val keyCountsB = sampledByKeyRDD.countByKey()
    val sizeB = keyCountsB.values.sum
    println(s"Sampled $sizeB examples using approximate stratified sampling (by label) ==> Approx Sample")
    
    //  Subsample, and count examples per label in sampled data. (approximate)
    val sampledByKeyRDDExact = keyedRDD.sampleByKeyExact(withReplacement = true, fractions = fractions)
    val keyCountsBExact = sampledByKeyRDDExact.countByKey()
    val sizeBExact = keyCountsBExact.values.sum
    println(s"Sampled $sizeBExact examples using exact stratified sampling (by label) ==> Exact Sample")

    //  Compare samples
    println(s"   \t<< Fractions of examples with key >>")
    println(s"Key\tOrig\tApprox Sample\tExact Sample")
    keyCounts.keys.toSeq.sorted.foreach { key =>
      val origFrac = keyCounts(key) / numExamples.toDouble
      val approxFrac = if (sizeB != 0) {
        keyCountsB.getOrElse(key, 0L) / sizeB.toDouble
      } else {
        0
      }
      val exactFrac = if (sizeBExact != 0) {
        keyCountsBExact.getOrElse(key, 0L) / sizeBExact.toDouble
      } else {
        0
      }
      println(s"$key\t$origFrac\t$approxFrac\t$exactFrac")
    }

    sc.stop()
  }
  
  
  
  /**
   * 3.4 check
   */
  def check() {
    val sc = new SparkContext("local", "check")
    
    val vec: Vector = Vectors.dense(Array(1.0, 2.0, 3.0)) // a vector composed of the frequencies of events
    // compute the goodness of fit. If a second vector to test against is not supplied as a parameter, 
    // the test runs against a uniform distribution.  
    val goodnessOfFitTestResult = Statistics.chiSqTest(vec)
    println(goodnessOfFitTestResult) // summary of the test including the p-value, degrees of freedom, 
    // test statistic, the method used, and the null hypothesis.
    
    val mat: Matrix = Matrices.dense(3, 2, Array(1.0,3.0,5.0,2.0,4.0,6.0)) // a contingency matrix
    // conduct Pearsons independence test on the input contingency matrix
    val independenceTestResult = Statistics.chiSqTest(mat)
    println(independenceTestResult) // summary of the test including the p-value, degrees of freedom...
    
    val lp = new LabeledPoint(1.0,vec)
    val obs: RDD[LabeledPoint] = sc.parallelize(Array(lp)) // (feature, label) pairs.
    // The contingency table is constructed from the raw (feature, label) pairs and used to conduct
    // the independence test. Returns an array containing the ChiSquaredTestResult for every feature 
    // against the label.
    val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(obs)
    var i = 1
    featureTestResults.foreach { result =>
      println(s"Column $i:\n$result")
      i += 1
    }
  }
  
  
  /**
   * 3.5 randomRdd
   */
  def randomRdd(){
    val sc = new SparkContext("local", "check")
    val rdd = RandomRDDs.normalRDD(sc, 10000, 1)
    val v = rdd.map { x => 1.0 + 2.0*x }
  }
}