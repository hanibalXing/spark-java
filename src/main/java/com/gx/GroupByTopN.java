package com.gx;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @author gx
 * @ClassName: GroupByTopN
 * @Description: java类作用描述
 * @date 2019/4/9 0:12
 * @Version: 1.0
 * @since
 */
public class GroupByTopN {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("spark-gx").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        context.textFile("C:\\Users\\pc\\Desktop\\sparkfile\\topn.txt")
                //先排序
                .mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(" ")[1]),s.split(" ")[0]))
                .sortByKey(false)
                .mapToPair(sortTuple->new Tuple2<>(sortTuple._2,sortTuple._1))
                //再分组
                .groupByKey()
                //再取前3
                .mapToPair(sortTuple->new Tuple2<>(sortTuple._1, IteratorUtils.toList(sortTuple._2.iterator()).subList(0,3)))
                .foreach(result->{
                    System.out.println(result._1);
                    result._2.forEach(System.out::println);
                });
        context.close();
    }

}
