package day1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Admin on 2017/5/15.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConf类（配置信息类），并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[2]")
    //创建SparkContext对象，该对象是提交Spark Application的入口
    val sc:SparkContext = new SparkContext(conf)
    //使用sc创建RDD并执行相应的transformation和action
    val lines: RDD[String] = sc.textFile(args(0))
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordAndOne: RDD[(String, Int)] = words.map((_,1))
    val count: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    val sortedCount: RDD[(String, Int)] = count.sortBy(_._2,false)

    //保存到HDFS
    sortedCount.foreach(println)
    //结束sc实例任务
    sc.stop()

  }
}
