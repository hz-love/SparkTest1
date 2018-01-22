package day4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Admin on 2017/5/18.
  */
object MySort {
  implicit val girlOrdering = new Ordering[Girl]{
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue > y.faceValue) 1
      else if(x.faceValue == y.faceValue){
        if(x.age > y.age)-1 else 1
      }else -1
    }
  }
}
object CustomSorDemo{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("CustomSort")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int, Int)] = sc.parallelize(List(("rumeng",92,15),("bobum",89,35),("angelaby",90,27),("liqing",90,28),("fanbin",91,32)))
    rdd1.sortBy(_._2,false)
    println(rdd1.collect().toBuffer)

  }
}

/**
  * 第一种方式
  *
  */
//case class Girl(faceValue:Int,age:Int)

/**
  * 第二种方式
  *
  */
case class Girl(val faceValue:Int,val age:Int) extends Ordered[Girl]{
  override def compare(that: Girl): Int = {
    if(this.faceValue==that.faceValue){
      that.age-this.age
    }else{
      this.faceValue-that.faceValue
    }
  }
}
