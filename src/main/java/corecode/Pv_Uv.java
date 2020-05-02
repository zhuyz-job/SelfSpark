package corecode;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class Pv_Uv {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("java_wc");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/pvuvdata");
        //pv
        JavaPairRDD<String, Integer> tupleIp = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.split("\t")[5], 1);
            }
        });

        JavaPairRDD<String, Integer> reduceResult = tupleIp.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> swap = reduceResult.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.swap();
            }
        });

        JavaPairRDD<Integer,String> result = swap.sortByKey(false);

        JavaPairRDD<String, Integer> end = result.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });

        end.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            @Override
            public void call(Tuple2<String,Integer> Tuple2) throws Exception {
                System.out.println(Tuple2);
            }
        });
    }
}
