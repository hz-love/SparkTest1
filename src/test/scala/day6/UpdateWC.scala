package day6

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object UpdateWC {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateWC")
    val sc = new SparkContext(conf)
    val scc = new StreamingContext(sc,Seconds(5))

    scc.checkpoint("e://ck0522")
    val lines: ReceiverInputDStream[String] = scc.socketTextStream("Node1",9999)
    val wordAndOne: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))
    val res: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateStateFunc,new HashPartitioner(scc.sparkContext.defaultParallelism),true)

    res.print()
    scc.start()

    scc.awaitTermination()

  }
  def updateStateFunc=(ite:Iterator[(String,Seq[Int],Option[Int])])=>{
    ite.flatMap{
      case (x,y,z) => Some(y.sum+z.getOrElse(0)).map(m=>(x,m))
    }
  }
}
