package corecode;


import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

class MySort2 implements Comparator<Integer>, Serializable {

    @Override
    public int compare(Integer o1, Integer o2) {
        return o2-o1;
    }
}
public class repartionAndSortWithinPartionsTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, String>(1, "zhangsan"),
                new Tuple2<Integer, String>(2, "lisi"),
                new Tuple2<Integer, String>(3, "zhangsan"),
                new Tuple2<Integer, String>(4, "zhangsan"),
                new Tuple2<Integer, String>(5, "lisi"),
                new Tuple2<Integer, String>(6, "wangwu")
        ), 2);

        rdd1.repartitionAndSortWithinPartitions(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                Integer k =Integer.valueOf(key.toString());
                return k%numPartitions();
            }

            @Override
            public int numPartitions() {
                return 3;
            }
        },new MySort2()).foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> st) throws Exception {
                System.out.println(st);
            }
        });
    }


}
