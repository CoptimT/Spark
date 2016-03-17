package cn.zxw.spark

import java.util.Random

/**
 * @author zhangxw
 */
object Example1 {
  def main(args: Array[String]): Unit = {
    val ranGen = new Random
    val byteArr = new Array[Byte](10)
    ranGen.nextBytes(byteArr)
    for(b <- byteArr){
      println(b)
    }
    println("-----------------------------------------")//test
    println(ranGen.nextInt(Int.MaxValue))
    println("-----------------------------------------")//test
  }
}