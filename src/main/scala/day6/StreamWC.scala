package day6

import org.apache.spark.streaming.dstream.{ReceiverInputDStream,DStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamWC {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamWC")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(20))

    //从TCP端口读取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("Node1",9999)
    //处理接收到的数据
    val res: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //调用DStream的打印方法
    res.print()
    //吧任务提交到集群上
    ssc.start()
    //一直运行，等待
    ssc.awaitTermination()
  }
}
