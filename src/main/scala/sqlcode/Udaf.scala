package sqlcode

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class MyUdaf() extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = StructType(Array[StructField](StructField("ee",StringType,true)))

  override def bufferSchema: StructType = StructType(Array[StructField](StructField("aa",IntegerType,true)))

  override def dataType: DataType = DataTypes.IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0)=0

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = buffer.update(0,buffer.getAs[Int](0)+1)

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
    buffer1.update(0,buffer1.getAs[Int](0)+buffer2.getAs[Int](0))

  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
}
object Udaf {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val list = List[String]("zhangsan", "lisi", "zhangsan", "zhangsan", "lisi", "wangwu", "wangwu")
    import session.implicits._
    val frame: DataFrame = list.toDF("name")
    frame.createTempView("mytable")
    session.udf.register("xxx", new MyUdaf())
    session.sql("select name,xxx(name) as count from mytable group by name").show()
  }
}