package com.bjsxt.mytestscalacode

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object Mysql {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("test").config("spark.sql.shuffle.partitions","1").getOrCreate()
    val propers = new Properties()
    propers.put("user","root")
    propers.put("password","123")
//    val df1: DataFrame = session.read.jdbc("jdbc:mysql://192.168.220.107:3306/test","person",propers)
    val df1: DataFrame = session.read.jdbc("jdbc:mysql://192.168.220.107:3306/test","(select person.id,person.name,score.score from person,score where person.id = score.id) tt",propers)
    //    df1.show()
    val rdd: RDD[Row] = df1.rdd
//    rdd.foreach(println)


//    val connectMap:Map[String,String] = Map[String,String](
//      "user" -> "root",
//      "password" -> "123",
//      "url" -> "jdbc:mysql://192.168.220.107:3306/test",
//      "dbtable" -> "score"
//      )
//    val df2: DataFrame = session.read.format("jdbc").options(connectMap).load()
    //    df2.show()
//    df1.createTempView("personInfo")
//    df2.createTempView("scoreInfo")
//    val resultFrame: DataFrame = session.sql("select a.id,a.name,a.age,b.score from personInfo a,scoreInfo b where a.id = b.id")
//    resultFrame.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.220.107:3306/test","result",propers)
  }
}
