package com.bjsxt.mytestscalacode

import org.apache.spark.sql.{DataFrame, SparkSession}

object Udf {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val list = List[String]("zhangs","lisi","wangu")
    import session.implicits._
    val df: DataFrame = list.toDF("name")
    df.createTempView("info")
    session.udf.register("LLEN",(name:String,n:Int)=>{
      name.length+n
    })
    session.sql("select name,LLEN(name,10) as length from info").show()
  }
}
