package day5

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 2017年5月20日21:20:49
  * 用Spark SQL将数据写入MySQL中
  */
object JdbcRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("JdbcRDD")
    val sc = new SparkContext(conf)
    val sqlContex = new SQLContext(sc)

    val file: RDD[String] = sc.textFile("E:\\person.txt")
    val personRDD: RDD[Array[String]] = file.map(_.split(" "))

    val schema: StructType = StructType {
      List(
        StructField("id", IntegerType, false),
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    }
    val rowRdd: RDD[Row] = personRDD.map(p => Row(p(0).toInt,p(1),p(2).toInt))
    val personDF: DataFrame = sqlContex.createDataFrame(rowRdd,schema)

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","WOAIYY")

    personDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/bigdata", "bigdata.person", prop)
    println("存储完毕")
    sc.stop()
  }
}
