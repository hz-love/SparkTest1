package day4

import java.sql
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Admin on 2017/5/18.
  */
object Sql2Data {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Sql2Data")
    val sc = new SparkContext(conf)

    val sql = "select * from location_info where counts > ?"
    def jdbcUtil(sql:String,paras:Array[String]):ArrayBuffer[ArrayBuffer[String]]= {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val ar = new ArrayBuffer[ArrayBuffer[String]]()
      try {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=utf8", "root", "WOAIYY")
        ps = conn.prepareStatement(sql)
        if (paras != null) {
          for (i <- 1 to paras.length) {
            ps.setString(i, paras(i - 1))
          }
        }
        val resultSet: ResultSet = ps.executeQuery()
        val data: ResultSetMetaData = resultSet.getMetaData
        val count: Int = data.getColumnCount

        while (resultSet.next()) {
          var arrs = new ArrayBuffer[String]()
          for (i <- 1 to count) {
            arrs += resultSet.getString(i)
          }
          ar += arrs
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (ps != null)
          ps.close()
        if (conn != null)
          conn.close()
      }
      ar
    }

    val util: ArrayBuffer[ArrayBuffer[String]] = jdbcUtil(sql,Array("500"))
    println(util)
    sc.stop()

  }
}