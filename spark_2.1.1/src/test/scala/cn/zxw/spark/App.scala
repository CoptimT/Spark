package cn.zxw.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.GZIPInputStream

object App{
    def main(args: Array[String]) {
        /*val v:Option[String] =  None
        val v1:Option[String] =  Option("12345")
        println( v.map(_.toLong) );
        println( v1.map(_.toLong) );
        println( v1 );
        println(decodeUnicode("\\u6ce2\\u6ce2\\u89c6\\u9891"));*/



        //val data="H4sIAAAAAAAAE21Ry27DIBD8lYqzZcB2SJxrvqCH9opowDEyMRYscZKq%2f9517ERJVYnD7jx2RuKbRN3JkwnR%2bp5s38gnz1nOSEb2%2brYrh0PeupifrfKdyuGEpDZ7r42Ey2BQVGQkBTepW4BhS%2bk%2fJlowvuasKGlZC8pFRbnvr5fOf%2bxqq8Zwfae21%2bacH8u0wQQ%2fmF5GCEYdJdjjFFOyOiONDRGkStr6B87FHT9ZbV7xpekfw%2fpBvDpKdMzSwanLA64xeBY%2bw1XF7vBybO%2bxw8IilwbnlZZgzpDCK7vKiFOH2xZn8e1yBAVpAUaroZ3SxSYjrbGHFnATU%2biXhaBgSdEJ5%2fn3SrFi1VS2NzD60MnG%2bRHxuuasQltqGhOeCk6Zy1asC7HB9%2fML5DzhdxICAAA%3d&clientIp=1.180.237.237&ngxTime=1508799600000&serverName=kafka02.yixia.com\ndata=H4sIAAAAAAAAA41SSU%2bDQBT%2bK2YOPRF47NCkMXGLiSaNHrR6IQPzKGOBQWboEuN%2fdyi0Wr14e9%2fy1pkPItkqWWMruajJ9Iw82SaYQAySsT0ut%2by97mODMMwEw0TtGtSKY5CuLXtLoVQztayD06w4FQ3lZiYqS6oWaWU9OvFmEeA9XM9v55I9X9ZvV3fVw6u6gSd2sXgxq8Y7l5IvZ%2bCgk6ZIo9x10txxIbUB3RyQQYoh8yeKV5hIRatmZvsQReCCF9hhNMln%2f2ujNxEN1skwWtLX01u4nm2QnLdSJbRjXHzzwYFfc4an%2fHiRXwnhUTjNCHWHwdqUdHekI9cgg%2fGEjuMDPRbLhJ5hVEEfvykFZYnCreraP2pJl3skB7ivrI%2bmupHYcKYKHcaxfscC%2bbJQGvmBllKuWqqGGTy9SqfR8DliCAG0o0a1Ee0qyUux0bTtgBP1iV2eY%2ftjiL7riGzfc8H%2f%2fALFXD0CbwIAAA%3d%3d"
        /*val data="H4sIAAAAAAAAA4WSUW%2fbIBDHv8rkhz65DtjYJpWiKWu7pdM0aarWPSJmzg5KMBRw0qzadx84TpfsZW%2f8f3fc%2fzjuNYEd9J5Jkdy8S8yWH8AmaWI2Heu5gggbrbKDfJE820kBGoT0OuYoLWAbE%2b6fltfLLxhFxvuh5Y0fbCgTQqvvyx%2f3DyGgXZS8F1YHq6jZDqyTuo%2b8zuJlbsw5rLI6yyNvzMDU%2bldkmKISoRM0VjfgnI5WNE26mHdqKjYDStsDa7jhjfSHQAuKi4qSNHFic271hDM0ttCIUe%2fdcx%2bPaSKgCRWZP5g4C5wmgx3Lr703N7PZlJgpybXhMguzmjlvgatZdfs4%2f%2fjpqyKP6Pnb3e21%2b7xcrZYPH%2b4IqYfOC4arTBny3jnZLaqyFKQGSgnJoSnaOfB8XgAgkhdtA%2b2VlwqY81yZBS4RpajAc1pX9Kpd%2fNdptIkzN9CzY3cs1gvvyFGdJq20zjM%2bCKn%2f8vmJj59%2bwaeZXF7A6C1weSMvws%2bOqXG73nAR8DHxEpcnPBVrdOhhigaLwWw1F8zDS9yxf6Nb3o3KHeVYOQzNDxPYS%2bHX4ViRINYgu7WPW1EF9VN6y32sU5DQgxiCOm5HhVEVXt2D32u7Ye1W7wOtwyKFW0Pbgj3rIFpOCpcU%2ff4D1cqx12ADAAA%3d"
        var res = URLDecoder.decode(data)
        val bytes: Array[Byte] = Base64.decodeBase64(res)
        res = uncompressToString(bytes)
        println(res)
        var appList: List[String] = null
        appList = List("12","34","56")
        println(appList.mkString("-"))*/

        var eventParams = Map[String, String]()
        eventParams = eventParams.+("eventId" -> "anti_cheating_event")
        eventParams = eventParams.+("antiCheatingCrypto" -> "ov1D6G/yUT/DE2biCgegVjJXelp6bpEK6xqAtxFDN")

        var eventId = eventParams.get("eventId")
        //解析applist
        if("anti_cheating_event".equals(eventId.get)){
            var appList: String = null
            if (eventParams.contains("antiCheating")) {
                appList = "antiCheating"
            } else if (eventParams.contains("antiCheatingCrypto")) {
                appList = "antiCheatingCrypto"
            }
            eventParams = eventParams - "antiCheating"
            eventParams = eventParams - "antiCheatingCrypto"
            eventParams = eventParams - "antiCheatingEncode"
            if(appList != null){
                eventParams = eventParams.+("appList" -> appList)
            }
        }
        println(eventParams)

        var list = List[String]()
        list = list.:+("aa")
        list = list.:+("bb")
        println(list)
    }

    @throws[Exception]
    def uncompressToString(b: Array[Byte]): String = {
        if (b == null || b.length == 0) {
            return null
        }
        val out: ByteArrayOutputStream = new ByteArrayOutputStream
        val in: ByteArrayInputStream = new ByteArrayInputStream(b)
        try {
            val gunzip: GZIPInputStream = new GZIPInputStream(in)
            val buffer: Array[Byte] = new Array[Byte](256)
            var n: Int = gunzip.read(buffer)
            while (n >= 0) {
                out.write(buffer, 0, n)
                n = gunzip.read(buffer)
            }
            return out.toString
        } catch {
            case e: Exception => {
                println(e.getLocalizedMessage)
            }
        }
        null
    }

    //Unicode转中文
    def decodeUnicode(dataStr:String)= {
        var start = 0
        var end = 0
        val buffer = new StringBuffer();
        while (start > -1) {
            end = dataStr.indexOf("\\u", start + 2);
            var charStr = "";
            if (end == -1) {
                charStr = dataStr.substring(start + 2, dataStr.length());
            } else {
                charStr = dataStr.substring(start + 2, end);
            }
            val letter = Integer.parseInt(charStr, 16).toChar; // 16进制parse整形字符串。
            buffer.append(letter.toString());
            start = end;
        }
        buffer.toString()
    }
}
