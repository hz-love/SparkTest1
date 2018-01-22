package day6

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamWC {
  def main(args: Array[String]): Unit = {
    LoggerLevel.setStreamingLogLevels()
    val conf: SparkConf = new SparkConf().setAppName("StreamWC").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val scc = new StreamingContext(sc,Seconds(20))

    //从TCP端口读取数据
    val lines: ReceiverInputDStream[String] = scc.socketTextStream("Node1", 9999)
    val res: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    res.print()
    scc.start()
    scc.awaitTermination()
  }
}
