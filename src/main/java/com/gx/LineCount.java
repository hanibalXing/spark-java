package com.gx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @author gx
 * @ClassName: LineCount
 * @Description: java类作用描述
 * @date 2019/4/8 16:44
 * @Version: 1.0
 * @since
 */
public class LineCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("spark-gx").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        context.textFile("C:\\Users\\pc\\Desktop\\sparkfile\\linecount.txt")
                .filter(s->s.contains("g"))
                .mapToPair((s) -> new Tuple2<>(s, 1))
                .reduceByKey((x, y) -> x + y)
                .mapToPair((tuple2)->new Tuple2<>(tuple2._2,tuple2._1))
                .sortByKey(false)
                .foreach((tuple2)-> System.out.println(tuple2._2+" appares "+tuple2._1));
        context.close();
    }
}
