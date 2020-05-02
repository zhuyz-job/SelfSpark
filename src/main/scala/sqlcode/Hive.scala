package com.bjsxt.mytestscalacode

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Hive {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").config("hive.metastore.uris","thrift://node04:9083").enableHiveSupport().getOrCreate()
//    val session = SparkSession.builder().appName("buildertest").enableHiveSupport().getOrCreate()
    session.sql("use spark")
    session.sql("drop table if exists person")
    session.sql("drop table if exists score")
    session.sql(
      """
        |create table  if not exists person (name string,age int) row format delimited fields terminated by '\t'
      """.stripMargin
    )
    session.sql(
      """
        |load data local inpath './data/student_infos' into table person
      """.stripMargin
    )
    session.sql(
      """
        |create table  if not exists score (name string,score int) row format delimited fields terminated by '\t'
      """.stripMargin
    )
    session.sql(
      """
        |load data local inpath './data/student_scores' into table score
      """.stripMargin
    )
//    val frame: DataFrame = session.table("score")
    val frame = session.sql("select a.name,a.age,b.score from person a,score b where a.name = b.name")
//    frame.show()
    frame.write.mode(SaveMode.Overwrite).saveAsTable("tt")
  }
}
