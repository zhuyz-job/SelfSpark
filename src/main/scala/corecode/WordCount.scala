package sqlcode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("scala-wc")
      val sc = new SparkContext(conf)
      val lines = sc.textFile("data/words")

      //算子
//      val result: RDD[String] = lines.filter(line => "hello hbase".equals(line))
//      result.foreach(println)
//      val result: RDD[String] = lines.sample(true,0.1,100L)
//      result.foreach(println)
//      val strings = lines.take(10)
//      var str = lines.first()
//      str.foreach(println)
//      val unit = sc.makeRDD(Array[Int](1,2,3,4,5,6))
//      unit.foreach(println)
      //正常
      val words: RDD[String] = lines.flatMap(line => {line.split(" ")})
      val pairWords = words.map(word=>{new Tuple2(word,1)})
      val reduceResult =pairWords.reduceByKey((v1:Int,v2:Int)=>{v1+v2})
      val result = reduceResult.sortBy(tp => {tp._2},false)
      //sortByKey
////      val result = reduceResult.map(tp =>{tp.swap}).sortByKey(false).map(tp => {tp.swap})
////      result.foreach(println)
//
//      //简化
////      val conf = new SparkConf().setAppName("scalawordcount").setMaster("local")
////      val sc = new SparkContext(conf)
////      sc.textFile("./data/words").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).sortBy(_._2,false).foreach(println)
  }
}
