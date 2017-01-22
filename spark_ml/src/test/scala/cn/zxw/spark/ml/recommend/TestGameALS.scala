package cn.zxw.spark.ml.recommend

import com.mongodb.client.model.UpdateOptions
import com.mongodb.{BasicDBObject, MongoClient, MongoClientURI}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

import scala.collection.mutable.ArrayBuffer


/**
 * Created by hadoop on 2015/11/2.
 */
object TestGameALS {
  //设置mongo连接参数
  val DB = "recommend_games"
  val MONGOURI = "mongodb://192.168.137.11:27017/" + DB
  val USER_COLLECTION = "user_to_games"
  val GAME_COLLECTION = "game_to_games"
  //设置向用户推荐多少款游戏
  var GAMELENGTH = 10

  def main(args: Array[String]) {
    //关闭干扰日志的输出
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //获取输入数据源
    //val file : String = args(0)
    //获取渠道ID
    //val channel : String = args(1)
    val channel = "82002a"

    //设置spark运行环境
    val conf = new SparkConf().setAppName("GameALS").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //加载HDFS中的用户评份数据
    val rowRating = sc.textFile("E:\\wanka\\ml-100k\\u.data").map(_.split("\t").take(3))
    //val rowRating = sc.textFile(file).map(_.split("\001"))

    val ratings = rowRating.map { case Array(user, game, rating) =>
      Rating(user.toInt, game.toInt, rating.toDouble)
    }.repartition(6).cache()

    //训练不同参数下的模型,获取最佳参数下的模型
    //模型中隐式因子的个数
    val ranks = List(50)
    //ALS的正则化参数
    val lambdas = List(0.005)
    //迭代次数
    val numIters = List(10)

    //spark模型库只包含基于矩阵分解的实现
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1

    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      //训练模型，使用ALS：交替最小二乘法
      val model = ALS.train(ratings, rank, numIter, lambda)
      //val model = ALS.trainImplicit(ratings, rank, numIter, lambda,5)
      //获取用户的真实打分
      val usersProducts = ratings.map { case Rating(user, product, rating) =>
        (user, product)
      }

      //获取预测的打分
      val predictions = model.predict(usersProducts).map {
        case Rating(user, product, rating) => ((user, product), rating)
      }

      val predictedAndTure = ratings.map {
        case Rating(user, product, rating) => ((user, product), rating)
      }.join(predictions).map {
        case ((user, product), (predicted, actural)) => (predicted, actural)
      }

      //得到均方差
      val validationRmse = new RegressionMetrics(predictedAndTure).rootMeanSquaredError

      println("Root Mean Squared Error = " + validationRmse + " for the model trained with rank = " + rank + " ,lambda = " + lambda
        + " ,and numIter = " + numIter)

      //获得最佳模型及该模型对应的参数
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }
    //打印bestModel时的参数值
    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda + " ,and numIter = "
      + bestNumIter
    )

    //优先验证模型中游戏的数量，如数量小于要取得的GAMELENGTH，则将GAMELENGTH设置为模型中的游戏数量
    val gameLength = bestModel.get.productFeatures.count()
    if (gameLength < GAMELENGTH) {
      GAMELENGTH = gameLength.toInt
    }

    //获取mongo的连接对象
    val mongoClient = getMongoCollection(MONGOURI, DB)

    //为每个用户推荐 GAMELENGTH 款游戏
    recommendGamesForUser(sc, mongoClient, bestModel.get, channel)

    //每款游戏推荐 GAMELENGTH 款相似游戏
    recommendGamesForGame(mongoClient, bestModel.get, channel)

    //关闭spark
    sc.stop()
    //关闭mongo连接
    mongoClient.close()
  }

  /**
   * 获取mongo的数据库连接
   * @param mongoURI
   * @param dbName
   * @return
   */
  def getMongoCollection(mongoURI: String, dbName: String) = {
    val uri = new MongoClientURI(mongoURI)
    new MongoClient(uri)
  }

  /**
   * 为每个用户推荐5款游戏
   * @param bestModel
   * 382:Rating(382,253,6.529992229187561) Rating(382,93,6.45736917187956) Rating(382,825,6.454823863130878) Rating(382,221,5.923307803684188) Rating(382,32,5.8906704573621536)
   */
  def recommendGamesForUser(sc: SparkContext, mongoClient: MongoClient, bestModel: MatrixFactorizationModel, channel: String): Unit = {
    //获取mongoDB
    val mongoDB = mongoClient.getDatabase(DB)
    //获取collection
    val collection = mongoDB.getCollection(USER_COLLECTION)

    val users: RDD[(Int, Array[Rating])] = bestModel.recommendProductsForUsers(GAMELENGTH)

    //加载用户ID的对照表
    val user_ids = sc.textFile("E:\\wanka\\ml-100k\\user_id.data").map(_.split("\001")).cache()
    for (user <- users.collect()) {
      //scala不提供break，只能通过标记来判断跳出循环
      var flag = false;
      for (user_id <- user_ids.collect()) {

        if (!flag) {
          if (user._1.toInt == user_id(0).toInt) {
            println(channel + "-" + user_id(1))
            val oba = new BasicDBObject("channel_cuid", channel + "-" + user_id(1))
            val obb = new BasicDBObject()
            val arrayBuf = ArrayBuffer[Int]()
            for (user <- user._2) {
              arrayBuf += user.product
            }
            obb.put("games", arrayBuf.toArray)
            val obc = new BasicDBObject("$set", obb)
            //使用mongo的有则更新、无则插入的模式
            val options = new UpdateOptions().upsert(true)
            collection.updateOne(oba, obc, options)
            flag = true
          }
        }
      }
    }

  }

  /**
   * 为每款游戏找到相似度最高的前10款游戏
   * @param bestModel
   */
  def recommendGamesForGame(mongoClient: MongoClient, bestModel: MatrixFactorizationModel, channel: String): Unit = {

    //获取mongoDB
    val mongoDB = mongoClient.getDatabase(DB)
    //获取游戏集合
    val collection = mongoDB.getCollection(GAME_COLLECTION)

    //得到游戏矩阵模型
    val games = bestModel.productFeatures.collect()
    //定义缓冲数组，用来存放所有游戏的相似游戏的前10位
    val datas = ArrayBuffer[String]()
    for (j <- 0 until games.length) {
      val itemVector = new DoubleMatrix(bestModel.productFeatures.lookup(games(j)._1).head)
      val sims = bestModel.productFeatures.map { case (id, factor) =>
        val factorVector = new DoubleMatrix(factor)
        val sim = cosineSimilarity(factorVector, itemVector)
        (id, sim)
      }
      //计算相似度时，因会把当前游戏本身取出计算，所以取出前GAMELENGTH +1 名，并去掉第1名
      val sortedSims = sims.top(GAMELENGTH + 1)(Ordering.by[(Int, Double), Double] {
        case (id, similarity) => similarity
      })
      //将当前游戏ID与相似度最高的5款游戏ID进行组合并加入到缓冲数组中
      //line格式为：gameId:90	(1007,0.7785901168875936)	(1098,0.7755617704677866)	(290,0.7667259503754023)	(64,0.7584299311418743)	(1119,0.7527691360533255)
      val line = "gameId:" + games(j)._1 + "\t" + sortedSims.take(GAMELENGTH + 1).takeRight(GAMELENGTH).mkString("\t")

      val gameArr = line.split("\t")
      //获取gameId
      val oba = new BasicDBObject("channel_game_id", channel + "-" + gameArr(0).split(":")(1))
      val obb = new BasicDBObject()

      val put_start = System.currentTimeMillis()
      val arrayBuf = ArrayBuffer[String]()
      for (i <- 1 to GAMELENGTH) {
        arrayBuf += gameArr(i).split(",")(0).replace("(", "")
      }
      //推荐游戏的id存放到obb中
      obb.put("games", arrayBuf.toArray)
      //设置要更新的数据集
      val obc = new BasicDBObject("$set", obb)
      //使用mongo的有则更新、无则插入的模式
      val options = new UpdateOptions().upsert(true)
      collection.updateOne(oba, obc, options)
    }
  }

  /**
   * 计算余铉相似度
   * @param vec1
   * @param vec2
   * @return
   */
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }

}
