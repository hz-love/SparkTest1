package day4

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Admin on 2017/5/18.
  */
object JdbcRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("jdbcRDD")
    val sc = new SparkContext(conf)

    val conn=()=>{
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=utf8", "root", "WOAIYY")
    }
    val sql="select * from location_info where counts >= ? and counts<= ?"
    val  jdbcR=new JdbcRDD(
      sc,conn,sql,500,2000,2,res=>{
        val location = res.getString(1)
        val counts=res.getInt(2)

        (location,counts)
      }
    )
    val collect: Array[(String, Int)] = jdbcR.collect
    println(collect.toBuffer)
    sc.stop()
  }
}
