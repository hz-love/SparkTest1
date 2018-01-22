package day7

import day6.LoggerLevels
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by liangdmaster on 2017/5/23.
  */
object KafkaWC {
  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map {
      case (x, y, z) => {
        (x, y.sum + z.getOrElse(0))
      }
    }
  }


  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args

    val conf: SparkConf = new SparkConf().setAppName("KafkaWC").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("e://ck20150523")
    //得到topic
    val topicMap: Map[String, Int] = topics.split(",").map((_, numThreads.toInt)).toMap
    //读取数据
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    //处理数据
    val wordAndOne: DStream[(String, Int)] = data.map(_._2).flatMap(_.split(" ")).map((_, 1))
    val res: DStream[(String, Int)] = wordAndOne.updateStateByKey(func, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    res.print()

    ssc.start()

    ssc.awaitTermination()

  }
}
