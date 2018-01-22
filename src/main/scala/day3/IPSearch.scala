package day3

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Admin on 2017/5/17.
  */
object IPSearch {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("IPSearch")
    val sc = new SparkContext(conf)

    //获取全国IP分配范围
    val file: RDD[String] = sc.textFile("E:\\SCALA\\day11\\IPSearch\\ip.txt")
    val IPinfo: Array[(String, String, String)] = file.map(x => {
      val fields = x.split("\\|")
      val startIP = fields(2)
      val endIP = fields(3)
      val province = fields(6)
      (startIP, endIP, province)
    }).collect()
    //将数据广播到所有属于这个APP的Executor上
    val broadcast: Broadcast[Array[(String, String, String)]] = sc.broadcast(IPinfo)
    //读取日志内容
    val provinceOne: RDD[(String, Int)] = sc.textFile("E:\\SCALA\\day11\\IPSearch\\http.log").map(x => {
      val fields = x.split("\\|")
      //IP地址
      val ip = fields(1)
      //ip地址转换成Long类型
      val ipLong: Long = iP2Long(ip)
      //获取广播变量的数据
      val arr = broadcast.value
      //得到IP地址的索引
      val index = binarySearch(arr, ipLong)
      //根据索引查找省份
      val provice = arr(index)._3

      (provice, 1)
    })
    val provinceCount: RDD[(String, Int)] = provinceOne.reduceByKey(_+_)

    for(i <- provinceCount.collect){
      println(i)
    }
    provinceCount.foreachPartition(data2Mysql)
    println("数据存储完毕")
    sc.stop()
  }

  //IP地址转换成LONG类型
  def iP2Long(ip:String):Long={
    val fragments=ip.split("\\.")
    var ipNum=0L
    for(i <- 0 until fragments.length){
      ipNum=fragments(i).toLong | ipNum << 8L //2的8次方26次方32次方
    }
    ipNum
  }
  //二分法检索
  def binarySearch(arr:Array[(String,String,String)],ip:Long):Int={
    var low=0
    var heigh=arr.length-1
    while (low<=heigh){
      var middle=(low+heigh)/2
      if((ip >= arr(middle)._1.toLong)&&(ip<= arr(middle)._2.toLong)){
        return middle
      }
      if(ip<arr(middle)._1.toLong){
        heigh=middle-1
      }
      else {
        low=middle+1
      }

    }
    -1
  }
  //将数据存储到Mysql
  val data2Mysql=(it:Iterator[(String,Int)]) => {
    var conn:Connection=null
    var ps:PreparedStatement=null
    val sql = "insert into location_info(location,counts,access_date) values(?,?,?)"

    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=utf8", "root", "WOAIYY")
      it.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    } catch {
      case e:Exception=>e.printStackTrace()
    } finally {
      if(ps!=null)
        ps.close()
      if(conn!=null)
        conn.close()
    }
  }
}
