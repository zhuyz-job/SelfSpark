package com.bjsxt.mytestscalacode

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Parquet {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val df1: DataFrame = session.read.format("json").load("./data/json")
    df1.write.mode(SaveMode.Append).parquet("./data/parquet")
    //    val df2: DataFrame = session.read.parquet("./data/parquet")
    val df2: DataFrame = session.read.format("parquet").load("./data/parquet")
    df2.show(100)
    val result = df2.count()
    println(result)
  }
}
