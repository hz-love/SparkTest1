package day2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Admin on 2017/5/16.
  * 在一段时间内，某个用户经常出没的地方
  * 按照时间顺序进行排序
  * 求停留时间最长的地方
  */
object MobileLocation {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.7.1")
    val conf: SparkConf = new SparkConf().setAppName("MobileLocation").setMaster("local")
    val sc = new SparkContext(conf)

    //获取数据
    val fileInfo: RDD[String] = sc.textFile("E:\\SCALA\\day10\\log")
    //切分
    val pnLacTime: RDD[((String, String), Long)] = fileInfo.map(line => {
      //切分
      val fields: Array[String] = line.split(",")
      //手机号
      val pn: String = fields(0)
      //时间
      val time: Long = fields(1).toLong
      //基站ID
      val lac: String = fields(2)
      //时间类型
      val eventType: String = fields(3)

      val time_log = if (eventType == "1") -time else time

      ((pn, lac), time_log)
    })


    //手机号用户在基站停留的时间
    val sumPLTime: RDD[((String, String), Long)] = pnLacTime.reduceByKey(_+_)

    //用户在不同基站停留时间
    val LPTime: RDD[(String, (String, Long))] = sumPLTime.map(x => {
      val pn = x._1._1
      val lac = x._1._2
      val timesum = x._2
      (lac, (pn, timesum))
    })
    //获取基站经纬度信息
    val lxy: RDD[(String, (String, String))] = sc.textFile("E:\\SCALA\\day10\\lac_info.txt").map(line => {
      //切分
      val fields = line.split(",")

      val lacID = fields(0)
      val x = fields(1)
      val y = fields(2)

      (lacID, (x, y))
    })

    //合并(lac, (pn, timesum))   (lacID,(x,y))
    val joinInfo: RDD[(String, ((String, Long), (String, String)))] = LPTime.join(lxy)


    //按照用户手机号分组并按时间取top2
    val res: RDD[(String, List[(String, Long, (String, String))])] = joinInfo.map(x => {
      val pn = x._2._1._1
      val lac = x._1
      val time = x._2._1._2
      val xy = x._2._2

      (pn, time, xy)
    }).groupBy(_._1).mapValues(_.toList.sortBy(_._2).reverse.take(2))
    for(i <- res.collect()){
      println(i)
    }

    sc.stop()
  }
}
