package corecode;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
class MySort3 implements Comparator<Integer>, Serializable {

    @Override
    public int compare(Integer o1, Integer o2) {
        return o2-o1;
    }
}
public class JoinPartion {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("join-test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 18),
                new Tuple2<String, Integer>("zhangsan", 19),
                new Tuple2<String, Integer>("zhangsan", 20),
                new Tuple2<String, Integer>("zhangsan", 21),
                new Tuple2<String, Integer>("zhangsan", 22),
                new Tuple2<String, Integer>("lisi", 23),
                new Tuple2<String, Integer>("wangwu", 24)
        ));

        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 100),
                new Tuple2<String, Integer>("zhangsan", 200),
                new Tuple2<String, Integer>("zhangsan", 20),
                new Tuple2<String, Integer>("lisi", 300),
                new Tuple2<String, Integer>("zhangsan", 22),
                new Tuple2<String, Integer>("wangwu", 400)
        ));

        /**
         * 取出rdd1中最多相同的key
         */
        String morekey =rdd1.sample(false, 1).mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2 tp) {
                return new Tuple2(tp._1, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tp) {
                return new Tuple2<>(tp._2, tp._1);
            }
        }).sortByKey(false).take(3).get(0)._2;

        /**
         * 从rdd1中分拆出最多key的数据
         */
        JavaPairRDD<String, Integer> rdd1_A = rdd1.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> v1) throws Exception {
                return morekey.equals(v1._1);
            }
        });
        /**
         * 从rdd1中分拆出最多key的其余数据
         */
        JavaPairRDD<String, Integer> rdd1_B = rdd1.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2 v1) throws Exception {

                return !morekey.equals(v1._1);
            }
        });
        /**
         * 从rdd2中分拆出最多key的数据
         */
        JavaPairRDD<String, Integer> rdd2_A = rdd2.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2 v1) throws Exception {

                return morekey.equals(v1._1);
            }
        });

        /**
         * 从rdd2中分拆出最多key的其余数据
         */
        JavaPairRDD<String, Integer> rdd2_B = rdd2.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2 v1) throws Exception {

                return !morekey.equals(v1._1);
            }
        });


        /**
         * 从rdd1中分拆出最多key的数据加前缀
         */
        JavaPairRDD<String, Integer> int_rdd1_A = rdd1_A.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            int i = 0;

            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tp) {
                if (i == 3) {
                    i = 0;
                }
                String newKey = i + "-" + tp._1;
                i += 1;
                return new Tuple2<>(newKey, tp._2);
                }
        });

        /**
         * 从rdd2中分拆出最多key的数据加前缀
         */
        JavaPairRDD<String, Integer> int_rdd2_A =  rdd2_A.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Integer> tp) {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    String newKey = i + "_" + tp._1;
                    list.add(new Tuple2<>(newKey, tp._2));
                }
                return list.iterator();
            }
        });

        /**
         * 从rdd1与rdd2中分拆最多key的数据进行join
         */
        JavaPairRDD<String, Tuple2<Integer, Integer>> join2 = int_rdd2_A.join(int_rdd1_A);


        /**
         * 从rdd1与rdd2中分拆最多key的数据join后去掉前缀
         */
        join2.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Tuple2<Integer, Integer>> tp) {
                String newKey = tp._1.substring(2);
                System.out.println(newKey);
                return new Tuple2<String, Tuple2<Integer, Integer>>(newKey, tp._2);

            }
        });

        /**
         * 从rdd1与rdd2中分拆最多key的其余数据进行join
         */
        JavaPairRDD<String, Tuple2<Integer, Integer>> join1 = rdd1_B.join(rdd2_B);

        /**
         * 两个join后的数据进行union
         */
        join1.union(join2).foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Integer>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);
            }
        });

    }

}
