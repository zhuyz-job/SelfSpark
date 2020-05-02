package com.bjsxt.mytestscalacode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JsonRdd {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val jsonList: List[String] = List[String](
      "{\"name\":\"zhangsan\",\"age\":\"18\"}",
      "{\"name\":\"lisi\",\"age\":\"19\"}",
      "{\"name\":\"wangwu\",\"age\":\"20\"}",
      "{\"name\":\"maliu\",\"age\":\"21\"}"
    )
    val jsonRDD: RDD[String] = session.sparkContext.parallelize(jsonList)
    import session.implicits._
    val unit: Dataset[String] = jsonList.toDS()
    val frame: DataFrame = session.read.json(unit)
    frame.createTempView("tt")
    session.sql("select sum(age) from tt").show()
  }
}
