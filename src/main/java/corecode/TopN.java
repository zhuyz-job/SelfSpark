package corecode;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;

public class TopN {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("java_wc");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/scores.txt");
        JavaPairRDD<String, Integer> rdd1 = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s.split("\t")[0], Integer.valueOf(s.split("\t")[1]));
            }
        });
        JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupByKey();
        rdd2.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tp) throws Exception {
                String cl = tp._1;
               Iterator<Integer> iter = tp._2.iterator();
                //定长数组方式
                Integer[] top3 = new Integer[3];
                while(iter.hasNext()) {
                    Integer score = iter.next();
                    for (int i = 0;i<top3.length;i++){
                        if(top3[i] == null){
                            top3[i] = score;
                            break;
                        }else if(score > top3[i]){
                            for(int j = 2;j > 1; j--){
                                top3[j] = top3[j-1];
                            }
                            top3[i] = score;
                            break;
                        }
                    }
                }
                System.out.println(cl);
                for(Integer it : top3){
                    System.out.println(it);
                }
                //list方式
//                List<Integer> list = IteratorUtils.toList(iter);
//                Collections.sort(list, new Comparator<Integer>() {
//                    @Override
//                    public int compare(Integer o1, Integer o2) {
//                        return o2-o1;
//                    }
//                });
//                for(Integer score : list){
//                    System.out.println(cl+" score ="+score);
//                }
            }
        });
    }
}
