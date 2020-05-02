package sqlcode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonFileToDf {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local").appName("test").getOrCreate();
        Dataset<Row> json = session.read().json("./data/json");
//        json.show();
//        json.printSchema();
//        Dataset<Row> ds1 = json.select("name", "age");
//        ds1.show();
        json.select(json.col("name"),json.col("age").plus(10).alias("addage")).show();
    }
}
