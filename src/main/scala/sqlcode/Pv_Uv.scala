package com.bjsxt.mytestscalacode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Pv_Uv1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc =new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("data/pvuvdata")
    //pv
    val tupleIp: RDD[(String, Int)] = lines.map(line=>{(line.split("\t")(5),1)})
//    tupleIp.reduceByKey(_+_).sortBy(tp=>tp._2,false).foreach(println)

    //uv

//    val unit: RDD[(String, String)] = lines.map(line=>{(line.split("\t")(0),line.split("\t")(5))}).distinct()
//    unit.map(one=>(one._2,1)).reduceByKey(_+_).sortBy(tp=>tp._2,false).foreach(println)

  }

}
