package cn.zxw.spark

object App{
    def main(args: Array[String]) {
        val v:Option[String] =  None
        val v1:Option[String] =  Option("12345")
        println( v.map(_.toLong) );
        println( v1.map(_.toLong) );
        println( v1 );
    }
}
