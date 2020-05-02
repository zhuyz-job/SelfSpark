package Streamingcode

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("test")
    val context: StreamingContext = new StreamingContext(conf,Durations.seconds(5))
    context.checkpoint("./data/ck")
    val lines: ReceiverInputDStream[String] = context.socketTextStream("node05",9999)
    val words: DStream[String] = lines.flatMap(one=>{one.split(" ")})
    val TupleWord: DStream[(String, Int)] = words.map({ one=>(one,1)})
    val result: DStream[(String, Int)] = TupleWord.reduceByKey((v1, v2)=>(v1+v2))
    val unit: DStream[(String, Int)] = result.updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
      var i: Int = option.getOrElse(0)
      for (a <- seq) {
        i += a
      }
      Option(i)
    })
    unit.print()
    context.start()
    context.awaitTermination()
  }
}
