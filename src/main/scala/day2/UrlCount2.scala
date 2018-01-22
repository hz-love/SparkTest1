package day2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Admin on 2017/5/16.
  */
object UrlCount2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("UrlCount2").setMaster("local")
    val sc = new SparkContext(conf)
    val projects: Array[String] = Array("http://java.learn.com", "http://ui.learn.com", "http://h5.learn.com", "http://bigdata.learn.com")

    //获取数据
    val file: RDD[String] = sc.textFile("E:\\SCALA\\day10\\access.txt")
    //每个URL后面记作1，生成一个元组
    val urlOne: RDD[(String, Int)] = file.map(x => {
      val fields = x.split("\t")
      val url = fields(1)
      (url, 1)
    })
    val sumURL: RDD[(String, Int)] = urlOne.reduceByKey(_+_)
    //将经常使用的数据缓存到内存
    val cache: RDD[(String, Int)] = sumURL.cache()
    for(pro <- projects){
      val filter: RDD[(String, Int)] = cache.filter(_._1.startsWith(pro))
      val res: Array[(String, Int)] = filter.sortBy(_._2,false).take(3)
      println(res.toBuffer)
    }

    sc.stop()
  }
}
