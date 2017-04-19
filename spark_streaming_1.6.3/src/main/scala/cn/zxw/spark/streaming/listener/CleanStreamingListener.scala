package cn.zxw.spark.streaming.listener

import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

class CleanStreamingListener extends StreamingListener{

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val batchInfo = batchCompleted.batchInfo
    //val batchTime = batchInfo.batchTime
    val numRecords = batchInfo.numRecords
    val schedulingDelay = batchInfo.schedulingDelay.get
    val processingStartTime = batchInfo.processingStartTime.get
    val processingEndTime = batchInfo.processingEndTime.get
    val totalDelay = batchInfo.totalDelay.get

    println("============bach 处理的条数 === new =====================" + numRecords + "============================")
    println("============bach 调度延迟 === new =====================" + schedulingDelay + "============================")
    println("============bach 开始时间 === new =====================" + processingStartTime + "============================")
    println("============bach 结束时间 === new =====================" + processingEndTime + "============================")
    println("============bach 总共延迟 === new =====================" + totalDelay + "============================")

  }
}
