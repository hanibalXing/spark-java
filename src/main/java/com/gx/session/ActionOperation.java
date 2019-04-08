package com.gx.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author gx
 * @ClassName: ActionOperation
 * @Description: java类作用描述
 * @date 2019/4/8 19:57
 * @Version: 1.0
 * @since
 */
public class ActionOperation {
    public static void main(String[] args) {
        groupBykey();
    }
    private static void groupBykey()
    {
        SparkConf conf = new SparkConf().setAppName("spark-gx").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> list= Arrays
                .asList(new Tuple2<>("class1",100),new Tuple2<>("class1",100),new Tuple2<>("class2",100)
                        ,new Tuple2<>("class3",100),new Tuple2<>("class2",100),new Tuple2<>("class3",100));
        JavaPairRDD<String, Integer> scores = context.parallelizePairs(list);
        context.parallelizePairs(list).countByKey().forEach((k,count)->{
            System.out.println(k);
            System.out.println(count);
        });
    }

}

