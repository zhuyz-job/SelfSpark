package sqlcode

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
case class info(date:String,category: String,money:Long)
object Owh {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val ds: Dataset[String] = session.read.textFile("./data/sales")
    import session.implicits._
    val unit: Dataset[info] = ds.map(one => {
      val date = one.split("\t")(0)
      val category = one.split("\t")(1)
      val money = one.split("\t")(2)
      info(date, category, money.toLong)
    })

    val frame: DataFrame = unit.toDF()
//    frame.show()
    frame.createTempView("sales")
    session.sql(
      """
        |select date,category,money,rk from
        |(select date,category,money,row_number() over(partition by category order by money desc) as rk from sales) temp
        |where temp.rk <=3
      """.stripMargin
    ).show(100)
  }
}
