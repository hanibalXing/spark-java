package com.gx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TransformationOperation {
    public static void main(String[] args) {
      //  Arrays.asList(1,3,4,5,6,7,8,9).stream().filter(i->i%2==0).forEach(System.out::println);
        //map();
        //filter();
        groupBykey();
    }

    private static void map() {
        SparkConf conf = new SparkConf().setAppName("spark-gx").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = context.parallelize(Arrays.asList(1, 3, 4, 5, 6, 7));

        JavaRDD<Integer> result = rdd.map((v)->v*2);

        result.foreach((v)-> System.out.println(v));
        context.close();
    }

    private static void groupBykey()
    {
        SparkConf conf = new SparkConf().setAppName("spark-gx").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> list=Arrays
                .asList(new Tuple2<>("class1",100),new Tuple2<>("class1",100),new Tuple2<>("class2",100)
                        ,new Tuple2<>("class3",100),new Tuple2<>("class2",100),new Tuple2<>("class3",100));
        JavaPairRDD<String, Integer> scores = context.parallelizePairs(list);
        context.parallelizePairs(list).groupByKey().foreach((s)->{
        System.out.println(s._1);
        s._2.forEach(System.out::println);
    });



}

    private static void filter() {
        SparkConf conf = new SparkConf().setAppName("spark-gx").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = context.parallelize(Arrays.asList(1, 3, 4, 5, 6, 7));
        JavaRDD<Integer> result = rdd.filter(
                new Function<Integer, Boolean>() {
                    @Override
                    public Boolean call(Integer integer) throws Exception {
                        return integer % 2 == 0;
                    }
                }
        );

        result.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


    }
}
