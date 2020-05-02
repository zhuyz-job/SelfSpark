package com.bjsxt.mytestscalacode

import org.apache.spark.sql.{DataFrame, SparkSession}

object Csv {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val frame: DataFrame = session.read.format("csv").option("header",true).load("./data/test.csv")
//    frame.show()
    frame.createTempView("tt")
    session.sql(
      """
        |select id,change,name,row_number() over (partition by id order by name) as rk from tt
      """.stripMargin
    ).createTempView("temp")
    session.sql(
      """
        |select t1.id,t1.change,t1.name from temp t1,temp t2
        |where t1.id = t2.id and t1.rk = t2.rk-1 and t1.change != t2.change
      """.stripMargin
    ).write.csv("./data/csvresult")
  }
}
