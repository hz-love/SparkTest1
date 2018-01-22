package day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by Admin on 2017/5/19.
  * 通过反射推断Schema信息获取数据
  */
object InferSchema {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("InferSchema")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //获取HDFS数据
    val lineRDD: RDD[Array[String]] = sc.textFile("hdfs://xiaoZH:9000/1603/person.txt").map(_.split(","))
    //关联RDD和case class
    val personRDD: RDD[Person] = lineRDD.map(p => Person(p(0).toInt,p(1),p(2).toInt))
    import sqlContext.implicits._
    //将RDD转换为DataFrame
    val personDF= personRDD.toDF()

    //注册成二维表
    personDF.registerTempTable("t_person")

    //传入SQL语句
    val sql: DataFrame = sqlContext.sql("select * from t_person where age < 26 order by age desc")

    sql.write.json("E://person.txt")

    sc.stop()
  }
}

case class Person(id:Int,name:String,age:Int)
