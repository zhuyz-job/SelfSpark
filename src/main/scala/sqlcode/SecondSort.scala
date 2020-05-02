package com.bjsxt.mytestscalacode

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
case class Mysort(first:Int,second:Int) extends Ordered[Mysort]{
  override def compare(that: Mysort): Int = {
    if(this.first == that.first){
      this.second - that.second
    }else(
      this.first - that.second
    )
  }
}
object SecondSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc =new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("data/secondSort.txt")
    val rdd1: RDD[(Mysort, String)] = lines.map(one=>{(Mysort(one.split(" ")(0).toInt,one.split(" ")(1).toInt),one)})
    rdd1.sortByKey(false).foreach(println)
  }
}
