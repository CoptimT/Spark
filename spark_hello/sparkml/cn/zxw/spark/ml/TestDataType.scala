package cn.zxw.spark.ml

import org.apache.spark.mllib.linalg.{ Vectors, Vector }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry

object TestDataType extends App {
  //2.1
  def testVector() {
    val v1: Vector = Vectors.dense(Array(1.0, 2.0, 3.0))
    /**
     *        长度               索引                                  索引对应值
     * sparse(size: Int, indices: Array[Int], values: Array[Double])
     */
    val v2: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    /**
     *        长度               Seq(索引 ,索引对应值)
     * sparse(size: Int, elements: Seq[(Int, Double)]): Vector
     */
    val v3: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
  }
  
  //2.2
  def testLabeledPoint(){
    val dense: Vector = Vectors.dense(Array(1.0, 2.0, 3.0))
    val sparse: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    //                    标签,向量
    val pos = LabeledPoint(1, dense)
    val neg = LabeledPoint(0, sparse)
  }
  
  //2.3
  def testSparseData(){
    val sc = new SparkContext("local","test")
    val data:RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "file/mllib/sample_libsvm_data.txt")
  }
  
  //2.4
  def testMatrices(){
    /**
     1.0 2.0
     3.0 4.0
     5.0 6.0
     */
    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    /**
     9.0 0.0
     0.0 8.0
     0.0 6.0
     */
    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
  }
  
  //2.5.1 行分布式矩阵
  def testRowMatrix(){
    val sc = new SparkContext("local","testRowMatrix")
    val v1: Vector = Vectors.dense(Array(1.0, 2.0, 3.0))
    val arr = Array(v1)
    val rows:RDD[Vector] = sc.parallelize(arr)
    val rm:RowMatrix = new RowMatrix(rows)
    val m = rm.numRows()
    val n = rm.numCols()
    println(m)//1
    println(n)//3
  }
  
  //2.5.2 行索引矩阵
  def testIndexedRowMatrix(){
    //1 2 3
    //2 3 4
    val sc = new SparkContext("local","testIndexedRowMatrix")
    val vector1: Vector = Vectors.dense(Array(1.0, 2.0, 3.0))
    val vector2: Vector = Vectors.dense(Array(2.0, 3.0, 4.0))
    val indexedRow1:IndexedRow = new IndexedRow(0,vector1)
    val indexedRow2:IndexedRow = new IndexedRow(1,vector2)
    val seq = Array(indexedRow2,indexedRow1)
    val rows:RDD[IndexedRow] = sc.parallelize(seq)
    val irm:IndexedRowMatrix = new IndexedRowMatrix(rows)
    val m = irm.numRows()
    val n = irm.numCols()
    val rm = irm.toRowMatrix()
    println(m)//2
    println(n)//3
  }
  
   //2.5.3 三元组矩阵
  def testCoordinateMatrix(){
    //1 2 3
    //4 5 6
    val sc = new SparkContext("local","testCoordinateMatrix")
    val matrixEntry1:MatrixEntry = new MatrixEntry(0,0,1)
    val matrixEntry2:MatrixEntry = new MatrixEntry(0,1,2)
    val matrixEntry3:MatrixEntry = new MatrixEntry(0,2,3)
    val matrixEntry4:MatrixEntry = new MatrixEntry(1,0,4)
    val matrixEntry5:MatrixEntry = new MatrixEntry(1,1,5)
    val matrixEntry6:MatrixEntry = new MatrixEntry(1,2,6)
    val seq = Array(matrixEntry1,matrixEntry2,matrixEntry3,matrixEntry4,matrixEntry5,matrixEntry6)
    val entries:RDD[MatrixEntry] = sc.parallelize(seq)
    val cm:CoordinateMatrix = new CoordinateMatrix(entries)
    val m = cm.numRows()
    val n = cm.numCols()
    val rm = cm.toRowMatrix()
    val irm = cm.toIndexedRowMatrix()
    println(m)//2
    println(n)//3
  }
  
}