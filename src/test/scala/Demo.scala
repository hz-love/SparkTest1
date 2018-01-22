import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Admin on 2017/5/18.
  */
object Demo {

  def main(args: Array[String]): Unit = {
    val line = "3g.donews.com 220.181.108.99 - - [17/Jan/2017:00:52:46 +0800] \"GET /News/donews_detail?id=2317077 HTTP/1.1\" \"http://3g.donews.com/News/donews_detail?id=2317077\" 200 970 \"-\" \"Mozilla/5.0 (Linux;u;Android 4.2.2;zh-cn;) AppleWebKit/534.46 (KHTML,like Gecko) Version/5.1 Mobile Safari/10600.6.3 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)\" \"-\" \"China\" \"22\" \"Beijing\""
    val strings: ArrayBuffer[String] = lineToGroup(line)
    print(strings.length)
    print(strings)
  }

  val regex = "^([\\S]+)\\s([\\S]+)\\W+\\[(.+)\\]\\s\"(.+)\"\\s\"(.+)\"\\s([\\S]+)\\s([\\S]+)\\s\"(.+)\"\\s\"(.+)\"\\s\"(.+)\"\\s\"(.+)\"\\s\"(.+)\"\\s\"(.+)\"$"

  //通过正则分组，获取字段。如果字段匹配成功且大于11的为有效数据
  def lineToGroup(line: String): ArrayBuffer[String] = {
    val groups = ArrayBuffer[String]()
    val p = Pattern.compile(regex)
    val m = p.matcher(line)
    while (m.find()) {
      for (i <- Range(1, m.groupCount() + 1, 1)) {
        groups.append(m.group(i))
      }
    }
    if(groups.length>=11){
      return groups
    }
    null
  }
}
