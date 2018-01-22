package day4


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Admin on 2017/5/18.
  */
object Custom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("CustomSort")
    val sc = new SparkContext(conf)

    val rdd1= sc.parallelize(List(("rumeng",92,15),("bobum",89,35),("angelaby",90,27),("liqing",90,28),("fanbin",91,32)))
    val rdd2: RDD[(String, Int, Int)] = rdd1.sortBy(_._2,false)
    println(rdd2.collect().toBuffer)

  }
}
