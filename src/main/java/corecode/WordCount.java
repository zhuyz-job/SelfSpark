package corecode;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("java_wc");
        JavaSparkContext sc = new JavaSparkContext(conf);
      JavaRDD<String> lines = sc.textFile("./data/words");


//        //filter
//        JavaRDD<String> filter = lines.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                return "hello hbase".equals(s);
//            }
//        });
//        filter.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        //随机抽样算子
//        JavaRDD<String> sample = lines.sample(true, 0.1, 100L);
//        sample.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        //action算子
//        List<String> collect = lines.collect();
//        for(String s:collect){
//            System.out.println(s);
//        }
//        String first = lines.first();
//        System.out.println(first);
//        List<String> take = lines.take(5);
//        for(String s: take){
//            System.out.println(s);
//        }
//        long count = lines.count();
//        System.out.println(count);



        //简化
//        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//        JavaPairRDD<String, Integer> pairWords = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
//        JavaPairRDD<String, Integer> result = pairWords.reduceByKey((v1,v2) -> v1+v2);
//        result.foreach(tuple2 -> System.out.println(tuple2));

        //正常
//        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String line) throws Exception {
//                String[] split = line.split(" ");
//                return Arrays.asList(split).iterator();
//            }
//        });
//
//        JavaPairRDD<String,Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<>(word,1);
//            }
//        });
//
//        JavaPairRDD<String,Integer> reduceResult = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        });
//
//        JavaPairRDD<Integer, String> swap = reduceResult.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
//
//            @Override
//            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
//                return tuple2.swap();
//            }
//        });
//
//        JavaPairRDD<Integer,String> result = swap.sortByKey(false);
//
//        JavaPairRDD<String, Integer> end = result.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
//
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
//                return integerStringTuple2.swap();
//            }
//        });
//
//        end.foreach(new VoidFunction<Tuple2<String,Integer>>() {
//            @Override
//            public void call(Tuple2<String,Integer> Tuple2) throws Exception {
//                System.out.println(Tuple2);
//            }
//        });
        sc.stop();
    }
}
