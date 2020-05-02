package corecode

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array[(String, Int)](
      ("zhangsan", 1),
      ("lisi", 2),
      ("zhangsan", 3),
      ("zhangsan", 4),
      ("lisi", 5),
      ("wangwu", 6)
    ), 2)

    //    rdd1.aggregateByKey(0)((s:Int,i:Int)=>(s+i),(s1:Int,s2:Int)=>(s1+s2)).foreach(println)
    rdd1.combineByKey((a: Int) => {
      a
    }, (s: Int, a: Int) => {
      s + a
    }, (s1: Int, s2: Int) => (s1 + s2)).foreach(println)
  }
}