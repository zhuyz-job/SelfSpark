package com.bjsxt.mytestscalacode

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadNestJson {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val df1: DataFrame = session.read.format("json").load("./data/nestJsonFile")
//    df1.printSchema()
//    df1.show(100)
    df1.createTempView("NestJson")
    session.sql("select name,score,infos.gender,infos.age from NestJson").show(100)
  }
}
