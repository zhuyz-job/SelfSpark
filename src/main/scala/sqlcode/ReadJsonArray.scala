package com.bjsxt.mytestscalacode

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadArrayJson {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val frame = session.read.json("./data/jsonArrayFile")
//    frame.show(false)
//    frame.printSchema()
    import session.implicits._
    import org.apache.spark.sql.functions._
    val TransDF: DataFrame = frame.select($"name", $"age", explode($"scores")).toDF("name", "age", "allScores")
//    TransDF.show()
    TransDF.printSchema()
    val df: DataFrame = TransDF.select($"name", $"age",
      $"allscores.yuwen" as "chinese",
      $"allscores.shuxue" as "math",
      $"allscores.yingyu" as "english",
      $"allscores.dili" as "geography",
      $"allscores.shengwu" as "biology",
      $"allscores.huaxue" as "chemistry"
    )
    df.printSchema()
    df.show(100)
  }
}
