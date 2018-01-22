package day5

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Admin on 2017/5/20.
  * 通过StructType直接指定Schema
  */
object StructTypeschema {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StructType")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //获取数据
    val file: RDD[String] = sc.textFile("hdfs://xiaoZH:9000/1603/person.txt")
    val lines: RDD[Array[String]] = file.map(_.split(","))
    //由STructType直接指定每一个字段的Schema
    val schma: StructType = StructType {
      List(
        StructField("id", IntegerType, false),
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    }

    //RDD映射到rowRDD
    val rowRDD: RDD[Row] = lines.map(p => Row(p(0).toInt,p(1),p(2).toInt))
    //把Schema信息应用到rowRDD上
    val personDF: DataFrame = sqlContext.createDataFrame(rowRDD,schma)
    //注册表
    personDF.registerTempTable("t_person")

    val sql: DataFrame = sqlContext.sql("select * from t_person")
    sql.write.mode("append").json("E://person")

    sc.stop()
  }
}
