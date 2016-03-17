package cn.zxw.spark.streaming.util

import java.util.Random
import scala.io.Source
import java.net.ServerSocket
import java.io.PrintWriter


/**
 * @author zhangxw
 */
object MySocketServer {
  
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      System.err.println("Usage: <filename> <port> <millisecond>")
      System.exit(1)
    }
    val filename = args(0)
    val port = args(1).toInt
    val millis = args(2).toLong
    
    val lines = Source.fromFile(filename, "UTF-8").getLines().toList
    val rows = lines.length
    
    val server = new ServerSocket(port)
    while(true){
      val client = server.accept()
      new Thread(){
        override def run(){
          println("Get client connected from : " + client.getInetAddress)
          val out = new PrintWriter(client.getOutputStream,true)
          while(true){
            Thread.sleep(millis)
            val content = lines(index(rows))
            println(">>> " + content)
            out.write(content + "\r\n")
            out.flush()
          }
          out.close()
          client.close()
        }
      }.start()
    }
  }
  
  def index(length:Int):Int={
    val random = new Random
    random.nextInt(length)
  }
}