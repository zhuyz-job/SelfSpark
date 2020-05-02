package Streamingcode

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object ReadSocket {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("test")
    val context: StreamingContext = new StreamingContext(conf,Durations.seconds(5))
    val lines: ReceiverInputDStream[String] = context.socketTextStream("node05",9999)
    val words: DStream[String] = lines.flatMap(one=>{one.split(" ")})
    val TupleWord: DStream[(String, Int)] = words.map({ one=>(one,1)})
    val result: DStream[(String, Int)] = TupleWord.reduceByKey((v1, v2)=>(v1+v2))
//    result.print()
    result.foreachRDD(rdd=>{
      println("广播变量")
      val rdd2: RDD[String] = rdd.map(tp =>{
        print("xxxxxxxxxxxxxxx"+tp)
        tp._1+"_"+tp._2
      })
      rdd2.count()
    })
    context.start()
    context.awaitTermination()
//    context.stop()
  }
}
