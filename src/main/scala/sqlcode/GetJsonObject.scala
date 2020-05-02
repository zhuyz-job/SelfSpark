package com.bjsxt.mytestscalacode

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object GetJsonObject {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val list: List[String] = List[String](
      "{\"name\":\"zhangsan\",\"age\":18}",
      "{\"name\":\"lisi\",\"age\":18}",
      "{\"name\":\"wangwu\",\"age\":18}",
      "{\"name\":\"zhaoliu\",\"age\":18}"
    )
    import session.implicits._
    val frame: DataFrame = list.toDF("infos")
    frame.printSchema()
    frame.show()
  //使用get_json_object() 可以获取其中某些列组成新的dataFrame
    import org.apache.spark.sql.functions._
    val df: DataFrame = frame.select(get_json_object($"infos", "$.name").as("name"),
      get_json_object($"infos", "$.age").cast(IntegerType).as("age"))
    df.show(100)
    df.printSchema()
  }
}
