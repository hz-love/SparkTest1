package day2

import java.net.URL
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by Admin on 2017/5/16.
  *  需求：每个学科访问的URL数量的top3
  *  按照学科把数据放到不同的分区器
  *  生产环境中不知道有多少学科
  *  需要自定义一个分区器
  */
object UrlCount3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("UrlCount3")
    val sc = new SparkContext(conf)
    val projects: Array[String] = Array("http://java.learn.com", "http://ui.learn.com", "http://h5.learn.com", "http://bigdata.learn.com")

    val file: RDD[String] = sc.textFile("E:\\SCALA\\day10\\access.txt")
    //每个URL后面记作1，生成一个元组
    val urlAndOne: RDD[(String, Int)] = file.map(x => {
      //切分
      val fields = x.split("\t")
      //得到URL
      val url = fields(1)
      (url, 1)
    })

    //以相同URL来聚合
    val sumedURL: RDD[(String, Int)] = urlAndOne.reduceByKey(_ + _)

    //分析出学科并缓存,chache()是属于transformation的算子
    val cachedProject: RDD[(String, (String, Int))] = sumedURL.map(x => {
      val url = x._1
      val project = new URL(url).getHost
      val count = x._2
      (project, (url, count))
    }).cache()
    //得到所有学科并去重
    val distinctProjects: Array[String] = cachedProject.keys.distinct().collect()

    //调用自定义分区器ProjectPartitioner得到分区号
    val partitioner: ProPartitioner = new ProPartitioner(distinctProjects)
    //进行分区
    val partitioned: RDD[(String, (String, Int))] = cachedProject.partitionBy(partitioner)

    //每一个分区里进行二次排序并取top3
    val finalResult: RDD[(String, (String, Int))] = partitioned.mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(3).iterator
    })

    finalResult.saveAsTextFile("c://out")

    sc.stop()
  }
}

class ProPartitioner(projects:Array[String]) extends Partitioner{
  //用来存放域名（学科）和分区好
  private val value = new mutable.HashMap[String, Int]
  //计数器
  var num =0
  //改所有学科分贝分区号
  for(pro <- projects){
    value += (pro -> num)
    num +=1
  }
  //分区数量
  override def numPartitions: Int = projects.length

  //分区号
  override def getPartition(key: Any): Int = {
    value.getOrElse(key.toString,0)
  }
}
