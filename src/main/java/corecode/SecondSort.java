package corecode;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondSort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("java_wc");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/secondSort.txt");
        JavaPairRDD<MySort, String> rdd1 = lines.mapToPair(new PairFunction<String, MySort, String>() {
            @Override
            public Tuple2<MySort, String> call(String line) {
                Integer first = Integer.valueOf(line.split(" ")[0]);
                Integer second = Integer.valueOf(line.split(" ")[1]);
                return new Tuple2<>(new MySort(first, second), line);
            }
        });
        rdd1.sortByKey(false).foreach(new VoidFunction<Tuple2<MySort, String>>() {
            @Override
            public void call(Tuple2<MySort, String> tp) throws Exception {
                System.out.println(tp._2);
            }
        });
    }
}
