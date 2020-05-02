package com.bjsxt.mytestscalacode

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Dataset, SparkSession}
case class userAcc(uid:String,date:String,duration:Long)
object OwhSum {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val ds1: Dataset[String] = session.read.textFile("./data/userAccInfo.txt")
    import session.implicits._
    val df1 = ds1.map(one => {
      userAcc(one.split("\t")(0), one.split("\t")(1), one.split("\t")(2).toLong)
    })

    /*
    * 统计每个用户访问的历史累计时长，要求同一用户每天都累加之前天数的值
    * */
    //使用开窗函数
    df1.createTempView("userAcc")
//    session.sql(
//      """
//        |select uid,date,sum(duration) over (partition by uid order by date asc) as sum_duration
//        |from userAcc
//      """.stripMargin
//    ).show(100)

  //使用原生API
    import org.apache.spark.sql.functions._
//    val spec: WindowSpec = Window.partitionBy("uid").orderBy($"date".asc)
//    df1.select($"uid",$"date",sum("duration").over(spec).alias("sum_duration"))
//      .show(100)

    /*
    * 统计每个用户访问的历史总时长
    * */
    //sql实现
//    session.sql(
//      """
//        |select uid,date,sum(duration) over (partition by uid) as sum_duration from userAcc
//      """.stripMargin
//    ).show(100)

    //原生API
    val spec: WindowSpec = Window.partitionBy("uid")
    df1.select($"uid",$"date",sum("duration").over(spec)
      .alias("sum_duration")).show(100)
  }
}
