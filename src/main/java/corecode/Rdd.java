package corecode;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

public class Rdd {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建RDD
//        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
//        Integer reduce = rdd1.reduce(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        });
//        System.out.println(reduce);


        JavaPairRDD<String, Integer> nameRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 18),
                new Tuple2<String, Integer>("zhangsan", 18),
                new Tuple2<String, Integer>("lisi", 19),
                new Tuple2<String, Integer>("wangwu", 20),
                new Tuple2<String, Integer>("zhaoliu", 21)
        ),2);
        //groupByKey
//        JavaPairRDD<String, Iterable<Integer>> result = nameRDD.groupByKey();
//        result.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
//            @Override
//            public void call(Tuple2<String, Iterable<Integer>> tp) throws Exception {
//                System.out.println(tp);
//            }
//        });

        //countByValue
//        Map<Tuple2<String, Integer>, Long> result = nameRDD.countByValue();
//        Set<Map.Entry<Tuple2<String, Integer>, Long>> entries = result.entrySet();
//        for(Map.Entry<Tuple2<String, Integer>, Long> entry:entries){
//            Tuple2<String, Integer> key = entry.getKey();
//            Long value = entry.getValue();
//            System.out.println("key = "+key+",value = "+value);
//        };
        //countByKey
//        Map<String, Long> Map = nameRDD.countByKey();
//        System.out.println(Map);

        //mapValues
//        JavaPairRDD<String, String> result = nameRDD.mapValues(new Function<Integer, String>() {
//            @Override
//            public String call(Integer i) throws Exception {
//                return i + "#";
//            }
//        });
//        result.foreach(new VoidFunction<Tuple2<String, String>>() {
//            @Override
//            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//                System.out.println(stringStringTuple2);
//            }
//        });

//        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(
//                "a1", "a2", "a3", "a4",
//                "b1", "b2", "b3", "b4",
//                "c1", "c2", "c3", "c4"
//        ),3);

        //mapPartitionsWithIndex
//        JavaRDD<String> rdd2 = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer integer, Iterator<String> iter) throws Exception {
//                List<String> list = new ArrayList<>();
//                while (iter.hasNext()) {
//                    String one = iter.next();
//                    list.add("rdd1 partition index = [" + integer + "],value = [" + one + "]");
//                }
//                return list.iterator();
//            }
//        }, true);
        //repartition & coalesce
//        JavaRDD<String> rdd3 = rdd2.repartition(2);
//        JavaRDD<String> rdd3 = rdd2.coalesce(4);
//        System.out.println("rdd2 partition length ="+rdd2.getNumPartitions());
//        System.out.println("rdd3 partition length ="+rdd3.getNumPartitions());
//
//        JavaRDD<String> rdd4 = rdd3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer integer, Iterator<String> iter) throws Exception {
//                List<String> list = new ArrayList<>();
//                while (iter.hasNext()) {
//                    String one = iter.next();
//                    list.add("rdd3 partition index = [" + integer + "],value = [" + one + "]");
//                }
//                return list.iterator();
//            }
//        }, true);
//        List<String> collect = rdd4.collect();
//        for(String s:collect){
//            System.out.println(s);
//        }

        //转换Map
//        Map<String, Integer> stringIntegerMap = nameRDD.collectAsMap();
//        Set<Map.Entry<String, Integer>> entries = stringIntegerMap.entrySet();
//        for(Map.Entry<String, Integer> entry:entries){
//            String key = entry.getKey();
//            Integer value = entry.getValue();
//            System.out.println("key ="+key+",value ="+value);
//        }
        JavaPairRDD<String, Integer> scoreRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 100),
                new Tuple2<String, Integer>("lisi", 200),
                new Tuple2<String, Integer>("wangwu", 300),
                new Tuple2<String, Integer>("tianqi", 400)
        ),3);
//        System.out.println("nameRDD patitions"+nameRDD.getNumPartitions());
//        System.out.println("scoreRDD partitions"+scoreRDD.getNumPartitions());

        //union
//        JavaPairRDD<String, Integer> union = nameRDD.union(scoreRDD);
//        union.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                System.out.println(stringIntegerTuple2);
//            }
//        });
//        System.out.println("union partitions"+union.getNumPartitions());

        //cogroup
        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroup = nameRDD.cogroup(scoreRDD);
        cogroup.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);
            }
        });

        //join
//        JavaPairRDD<String, Tuple2<Integer, Integer>> join = nameRDD.join(scoreRDD);
//        System.out.println("join partitions"+join.getNumPartitions());
//        join.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Integer>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
//                System.out.println(stringTuple2Tuple2);
//            }
//        });

        //leftOuterJoin
//        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> stringTuple2JavaPairRDD = nameRDD.leftOuterJoin(scoreRDD);
//        stringTuple2JavaPairRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Optional<Integer>>>>() {
//
//            @Override
//            public void call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> tp) throws Exception {
////                System.out.println(tp);
//                String key = tp._1;
//                Integer it = tp._2._1;
//                Optional<Integer> opt = tp._2._2;
//                if(opt.isPresent()){
//                    System.out.println("key = "+key+",value1 = "+it+",value2 ="+opt);
//                }else{
//                    System.out.println("key = "+key+",value1 = "+it+",value2 = Null");
//                }
//
//            }
//        });

        //rightOutJoin
//        JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> rj = nameRDD.rightOuterJoin(scoreRDD);
//        rj.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>, Integer>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Optional<Integer>, Integer>> Tuple2) throws Exception {
//                System.out.println(Tuple2);
//            }
//        });

        //fullOutJoin
//        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<Integer>>> fj = nameRDD.fullOuterJoin(scoreRDD);
//        fj.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>, Optional<Integer>>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Optional<Integer>, Optional<Integer>>> stringTuple2Tuple2) throws Exception {
//                System.out.println(stringTuple2Tuple2);
//            }
//        });

//       JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "c", "b"));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(4,5,6));
        //zipWithIndex
//        JavaPairRDD<String, Long> rdd3 = rdd1.zipWithIndex();
//        rdd3.foreach(new VoidFunction<Tuple2<String, Long>>() {
//            @Override
//            public void call(Tuple2<String, Long> stringLongTuple2) throws Exception {
//                System.out.println(stringLongTuple2);
//            }
//        });
        //zip
//        JavaPairRDD<String, Integer> zip = rdd1.zip(rdd2);
//        zip.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                System.out.println(stringIntegerTuple2);
//            }
//        });

//        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h"));
//        System.out.println(rdd2.countByValue());

        //top
//        System.out.println(rdd1.top(3));

        //takeOrdered
//        System.out.println(rdd1.takeOrdered(3));

        //takeSample
//        System.out.println(rdd2.takeSample(false,3,100L));

        //去重
//        JavaRDD<String> distinct = rdd1.distinct();
//        distinct.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        //取差集
//        JavaRDD<String> subtract = rdd2.subtract(rdd1);
//        subtract.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        //取交集
//        JavaRDD<String> intersection = rdd1.intersection(rdd2);
//        intersection.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        //mapPartititons
//        JavaRDD<String> map = rdd1.map(new Function<String, String>() {
//            @Override
//            public String call(String s) throws Exception {
//                System.out.println("建立数据库连接");
//                System.out.println("插入数据" + s);
//                System.out.println("关闭数据库连接");
//                System.out.println("--------------");
//                return s + "#";
//            }
//        });
//        map.count();
//
//        JavaRDD<String> stringJavaRDD = rdd1.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
//            @Override
//            public Iterator<String> call(Iterator<String> itr) throws Exception {
//                List<String> list = new ArrayList<>();
//                System.out.println("建立数据库连接");
//                while (itr.hasNext()) {
//                    String one = itr.next();
//                    System.out.println("插入数据库" + one);
//                    list.add(one);
//                }
//                System.out.println("关闭数据库连接");
//                return list.iterator();
//            }
//        });
//        stringJavaRDD.count();
        //foreachPartition
//        rdd1.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println("建立数据库连接");
//                System.out.println("插入数据" + s);
//                System.out.println("关闭数据库连接");
//                System.out.println("--------------");
//            }
//        });
//        rdd1.foreachPartition(new VoidFunction<Iterator<String>>() {
//            @Override
//            public void call(Iterator<String> stringIterator) throws Exception {
//                System.out.println("建立数据库连接");
//                while (stringIterator.hasNext()) {
//                    String one = stringIterator.next();
//                    System.out.println("插入数据库" + one);
//                }
//                System.out.println("关闭数据库连接");
//            }
//        });
    }
}
