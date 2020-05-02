package corecode;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.AccumulatorV2;

import java.io.Serializable;
import java.util.Arrays;

class PersonInfo implements Serializable {
    Integer totalPersonCount;
    Integer totalAgeCount;

    public PersonInfo(Integer totalPersonCount, Integer totalAgeCount) {
        this.totalPersonCount = totalPersonCount;
        this.totalAgeCount = totalAgeCount;
    }
}

class MyAcc extends AccumulatorV2<PersonInfo,PersonInfo>{
    PersonInfo pi = new PersonInfo(0,0);
    @Override
    public boolean isZero() {
        return pi.totalPersonCount == 0 && pi.totalAgeCount ==0;
    }

    @Override
    public AccumulatorV2<PersonInfo, PersonInfo> copy() {
        MyAcc acc = new MyAcc();

        return acc;
    }

    @Override
    public void reset() {
        pi = new PersonInfo(0,0);
    }

    @Override
    public void add(PersonInfo v) {
        pi.totalPersonCount += v.totalPersonCount;
        pi.totalAgeCount += v.totalAgeCount;
    }

    @Override
    public void merge(AccumulatorV2<PersonInfo, PersonInfo> other) {
        MyAcc pa = (MyAcc)other;
        pi.totalPersonCount += pa.pi.totalPersonCount;
        pi.totalAgeCount += pa.pi.totalAgeCount;
    }

    @Override
    public PersonInfo value() {
        return pi;
    }
};
public class DefinedAcc {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> nameInfo = sc.parallelize(Arrays.asList(
                "zhangsan 1", "lisi 2", "wangwu 3", "zhaoliu 4", "tianqi 5"
        ), 3);

        MyAcc acc = new MyAcc();
        sc.sc().register(acc,"myacc");

        nameInfo.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                acc.add(new PersonInfo(1, Integer.parseInt(v1.split(" ")[1])));
                return v1;
            }
        }).collect();
        Integer totalAgeCount = acc.value().totalAgeCount;
        Integer totalPersonCount = acc.value().totalPersonCount;
        System.out.println(totalPersonCount + "-"+ totalAgeCount);


    }

}
