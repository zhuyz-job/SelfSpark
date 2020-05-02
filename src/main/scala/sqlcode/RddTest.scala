package com.bjsxt.mytestscalacode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Rdd {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("test")
      val sc =new SparkContext(conf)
    val rdd1 = sc.parallelize(List[String]("a 1","b 2","c 3","d 4"))
    val rdd2 = sc.parallelize(List[String]("a","b","c","e"))
    val unit: RDD[String] = rdd1.map(one=>{one.split(" ")(1)})
    unit.foreach(println)
//    val unit2: RDD[String] = rdd1.flatMap(one=>one.split(" "))
//    unit2.foreach(println)
    //去重
//    rdd1.map(one => {(one,1)}).reduceByKey(_+_).map(one => one._1).foreach(println)
//    rdd1.distinct().foreach(println)

    //takeOrdered
//    rdd2.takeOrdered(2).foreach(println)

    //top
//    rdd2.top(2).foreach(println)

    //takeSample
//    rdd1.takeSample(true,2,100L).foreach(println)

    //foreachPartition
//    rdd2.foreachPartition(iter=>{
//      println("建立数据库连接")
//      while(iter.hasNext){
//        val one = iter.next
//        println("插入数据库"+one)
//      }
//      println("关闭数据库连接")
//    })

    //mapPartitions
//    val unit = rdd1.mapPartitions(iter => {
//      val list = new ListBuffer[String]()
//      println("建立数据库连接")
//      while (iter.hasNext) {
//        val one = iter.next
//        println("插入数据库" + one)
//      }
//      println("关闭数据库连接")
//      list.iterator
//    })
//    unit.foreach(println)

    //interection & subtract
//    rdd1.intersection(rdd2).foreach(println)
//    rdd2.subtract(rdd1).foreach(println)

//    val nameRDD = sc.parallelize(Array[(String,Int)](
//        ("zhangsan", 18),
//        ("lisi", 19),
//        ("wangwu", 20),
//        ("zhaoliu", 21)
//    ),2);
//    val scoreRDD = sc.parallelize(Array[(String,Int)](
//      ("zhangsan", 100),
//      ("lisi", 200),
//      ("wangwu", 300),
//      ("tianqi", 400)
//    ),3);

    //join
//    val value: RDD[(String, (Int, Int))] = nameRDD.join(scoreRDD)
//    value.foreach(println)

    //LeftOutJoin
//    val unit: RDD[(String, (Int, Option[Int]))] = nameRDD.leftOuterJoin(scoreRDD)
//    unit.foreach(println)

    //rightOutJoin
//    val unit = nameRDD.rightOuterJoin(scoreRDD)
//    unit.foreach(println)

    //fullOutJoin
//    val unit: RDD[(String, (Option[Int], Option[Int]))] = nameRDD.fullOuterJoin(scoreRDD)
//    unit.foreach(println)

    //union
//    val unit: RDD[(String, Int)] = nameRDD.union(scoreRDD)
//    unit.foreach(println)

    //cogroup
//    val unit = nameRDD.cogroup(scoreRDD)
//    unit.foreach(println)

//    val unit: RDD[String] = sc.parallelize(Array[String](
//      "a1", "a2", "a3", "a4",
//      "b1", "b2", "b3", "b4",
//      "c1", "c2", "c3", "c4"
//    ), 3)

    //countByKey
//    val stringToLong: collection.Map[String, Long] = unit.zip(unit).countByKey()
//    stringToLong.foreach(println)

    //countByvalue
//    val stringToLong1: collection.Map[String, Long] = unit.countByValue()
//    stringToLong1.foreach(println)

    //mapValues
//    unit.map(one=>{(one,1)}).mapValues(one=>{one+100}).foreach(println)

    //zipWithIndex
//    val unit1: RDD[(String, Long)] = unit.zipWithIndex()
//    unit1.foreach(println)

    //groupByKey & zip
//    unit.zip(unit).groupByKey().foreach(println)

    //coalesce
//    unit.coalesce(4,true).foreach(println)

    //repartition
//    unit.repartition(4).foreach(println)

    //mapPartitionsWithIndex
//    unit.mapPartitionsWithIndex((index,iter)=>{
//      iter.map(one=>{
//        s"unit partition index = $index,value = $one"
//      })
//    }).foreach(println)
  }
}
