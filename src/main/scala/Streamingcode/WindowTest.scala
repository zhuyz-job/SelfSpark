package Streamingcode

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WindowTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("window")
    val context: StreamingContext = new StreamingContext(conf, Durations.seconds(5))
    context.sparkContext.setLogLevel("Error")
    context.checkpoint("./data/ck")
    val lines: ReceiverInputDStream[String] = context.socketTextStream("node05", 9999)
    val words: DStream[String] = lines.flatMap(one => {one.split(" ")})
    val tupleWord: DStream[(String, Int)] = words.map({ one => (one, 1) })

//    val result: DStream[(String, Int)] = tupleWord.reduceByKeyAndWindow((v1:Int, v2:Int)=>
//    {v1+v2},Durations.seconds(15),Durations.seconds(5))
    //优化机制
    val result: DStream[(String, Int)] = tupleWord.reduceByKeyAndWindow((v1:Int, v2:Int)=>
      {v1+v2},(v1:Int, v2:Int)=> {v1-v2},Durations.seconds(15),Durations.seconds(5))
    result.print()
    context.start()
    context.awaitTermination()
  }
}
