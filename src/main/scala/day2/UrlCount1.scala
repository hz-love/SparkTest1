package day2

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Admin on 2017/5/16.
  * 需求：每个学科访问的URL数量的TOP3
  */
object UrlCount1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("UrlCount1")
    val sc = new SparkContext(conf)

    //获取数据
    val file: RDD[String] = sc.textFile("E:\\SCALA\\day10\\access.txt")
    //每个URL后面记作1，生成 一个元组
    val urlOne:RDD[(String,Int)]=file.map(x =>{
      val fields=x.split("\t")
      //得到URL
      val url=fields(1)
      (url,1)
    })

    //把相同的URL聚合
    val sumUrl: RDD[(String, Int)] = urlOne.reduceByKey(_+_)
    //得到学科信息
    val projectInfo: RDD[(String, String, Int)] = sumUrl.map(x => {
      val url = x._1
      val count = x._2
      val project = new URL(url).getHost
      (project, url, count)
    })

    //以学科分组，得到最终结果
    val res: RDD[(String, List[(String, String, Int)])] = projectInfo.groupBy(_._1).mapValues(_.toList.sortBy(_._3).reverse.take(3))
    for(i <- res.collect){
      println(i)
    }
    sc.stop()
  }
}
