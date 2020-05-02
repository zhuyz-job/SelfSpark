package com.bjsxt.mytestscalacode

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class person(id:Int,name:String,age:Int,score:Double)

object ClassRdd {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val sc: SparkContext = session.sparkContext
    val unit: RDD[String] = sc.textFile("./data/people.txt")
    //动态创建Schema方式将普通RDD加载成DataFrame
    val rowRdd: RDD[Row] = unit.map(line => {
      val id = line.split(",")(0)
      val name = line.split(",")(1)
      val age = line.split(",")(2)
      val score = line.split(",")(3)
      Row(id.toInt, name, age.toInt, score.toDouble)
    })
    val structType: StructType = StructType(Array[StructField](
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("score", DoubleType, true)
    ))
    val frame: DataFrame = session.createDataFrame(rowRdd,structType)
    frame.show()

    //讲普通RDD通过类反射方式加载成DataFrame
//    val personRdd: RDD[person] = unit.map(line => {
//      val id = line.split(",")(0)
//      val name = line.split(",")(1)
//      val age = line.split(",")(2)
//      val score = line.split(",")(3)
//      person(id.toInt, name, age.toInt, score.toDouble)
//    })
//    import session.implicits._
//    val df: DataFrame = personRdd.toDF()
//    df.show()
//    df.printSchema()
//    df.createTempView("tt")
//    session.sql("select sum(score) as totalScore from tt ").show()

//    val unit1: Dataset[String] = df.map((row: Row) => {
//      s"id = ${row(0)} ,name = ${row(1)} ,age = ${row(2)} ,score = ${row(3)}"
//    })
//    unit1.show(false)
//    val unit2: Dataset[String] = df.map((row: Row) => {
//      val id = row.getAs[Int]("id")
//      val name = row.getAs[String]("name")
//      val age = row.getAs[Int]("age")
//      val score = row.getAs[Double]("score")
//      s"id = $id ,name = $name ,age = $age ,score = $score"
//    })
//    unit2.show(false)
  }
}
