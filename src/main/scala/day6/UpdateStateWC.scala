package day6

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 当前批次和历史数据进行累加——2017年5月22日14:33:05
  */
object UpdateStateWC {
  def main(args: Array[String]): Unit = {
    //设置log4j的日志输出级别
    LoggerLevels.setStreamingLogLevels()
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateWC")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    
    //如果是集群模式，检查点的地址应该设为hdfs的地址
    ssc.checkpoint("e://ck0522")
    //读取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("Node1",9999)
    //处理数据生成元组
    val wordAndOne: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))
    //批次累加需要用updateStateByKey方法
    val res: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    //打印结果
    res.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * String : 单词 hello
    * Seq[Int] ：单词在当前批次出现的次数
    * Option[Int] ： 历史结果
    */
  val updateFunc: (Iterator[(String, Seq[Int], Option[Int])]) => Iterator[(String, Int)] = (iter:Iterator[(String,Seq[Int],Option[Int])])=>{
    iter.flatMap{
      case (x,y,z) => Some(y.sum+z.getOrElse(0)).map(m=>(x,m))
    }
  }
}
