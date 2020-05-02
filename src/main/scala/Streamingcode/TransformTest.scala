package Streamingcode

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object TransformTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("test")
    val context: StreamingContext = new StreamingContext(conf, Durations.seconds(5))
    context.sparkContext.setLogLevel("Error")
    val lines: ReceiverInputDStream[String] = context.socketTextStream("node05", 9999)
    val result: DStream[String] = lines.transform(rdd => {
      val value: RDD[String] = rdd.filter(one => {
        !"zhangsan".equals(one.split(" ")(1))
      })
      value
    })
    result.print(100)
    context.start()
    context.awaitTermination()
    context.stop()
  }
}
