package cn.zxw.spark.streaming.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
 * 自定义MyHiveContext，实时在streaming中HiveContext的单例模式
 * Created by hadoop on 2016/1/16.
 */
class MyHiveContext{

}

object MyHiveContext {

  val myHiveContext = new MyHiveContext()

  /**
   * The active SQLContext for the current thread.
   */
  private val activeContext: InheritableThreadLocal[HiveContext] =
    new InheritableThreadLocal[HiveContext]

  def getOrCreate(sparkContext: SparkContext): HiveContext = {
    var ctx = activeContext.get()
    if (ctx != null && !ctx.sparkContext.isStopped) {
      return ctx
    }

    synchronized {
      //if (ctx == null || ctx.sparkContext.isStopped)
      if (ctx == null  || ctx.sparkContext.isStopped) {
        ctx = new HiveContext(sparkContext)
        activeContext.set(ctx)
        ctx
      } else {
        ctx
      }
    }
  }

  def setActive(hiveContext: HiveContext): Unit = {
    activeContext.set(hiveContext)
  }

  def clearActive(): Unit = {
    activeContext.remove()
  }

  def getActive(): Option[HiveContext] = {
    Option(activeContext.get())
  }
}
